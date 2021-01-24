from bibliograph.nasaads import query_ads
from bibliograph.nasaads import query_bibcodes
from bibliograph.util import backup
from bibliograph.util import compare_graph
from bibliograph.util import compare_overlap
from bibliograph.util import get_graph_link_labels
from bibliograph.util import get_overlap
from bibliograph.util import get_updated_entry
from bibliograph.util import merge_rows
from networkx import read_graphml
from networkx import write_graphml
from numbers import Number
from pandas import DataFrame
from pandas import Series
from pandas import read_json


class CitNet:
    '''
    The citnet class provides a citation network object that contains
    a pandas DataFrame for a bibliography, a DataFrame for references
    between bibliography, and a NetworkX graph representing the
    network.

    Possible Keywords
    -----------------
    bibcols : list-like
        List of labels for bibliography columns. Passed directly to
        pd.Dataframe constructor as 'columns' keyword argument.
        Required when initializing from bibtex.

    refcols : list-like
        Labels of columns whose values should be joined by spaces to
        create a unique reference string for each row. Required when
        initializing from bibtex.

    citcols : list-like
        Optional additional columns to store metadata for citation
        network edges.

    bibtex : string
        Name of BibTex file containing the bibliography

    data : ndarray, Iterable, dict, or DataFrame
        Optional data to initialize bibliography. Passed directly to
        pd.DataFrame constructor.

    index : Index or array-like
        Optional index to initialize bibliography. Passed directly to
        pd.DataFrame constructor.

    bibtex : string
        Name of BibTex file containing data for the bibliography.

    csv : string
        Name of csv file containing sources and targets. If bibcols
        and refcols are None, this function reads bibcols and refcols
        from the first two lines of the csv file.

    fileprefix : string
        Prefix of files containing saved CitNet data.

    bibtex_parsers : dictionary
        bibtex_parsers is a dictionary with format

            {tag:[col, function_to_process_bibtex]}

        or

            {tag:[[col1, function1_to_process_bibtex],
                  [col2, function2_to_process_bibtex]]}

        where tag is a field in the BibTex entries, col
        is the label for a bibliography column where data from that
        tag should be stored, and the functions translate the text of
        the BibTex entry into a value that should be stored in the
        bibliography column.
    '''

    def __init__(self, bib_cols, cit_cols=['src','tgt']):

        self.bib = DataFrame(columns=bib_cols)
        self.cit = DataFrame(columns=cit_cols)

    def __getattr__(self, attr):

        if attr in self.__dict__:
            return getattr(self, attr)
        return getattr(self.bib, attr)

    def __getitem__(self, item):

        return self.bib[item]

    def __setitem__(self, item, data):

        self.bib[item] = data

    def append(self, *args, **kwargs):
        '''
        Wraps pd.DataFrame.append so append on CitNet appends to the
        bib DataFrame. This is implemented because pd.DataFrame.append returns
        a new DataFrame and the use pattern for CitNet should be
            cn.append(data)
        rather than
            cn.bib = cn.append(data)
        '''
        self.bib = self.bib.append(*args, **kwargs)

    def import_documents(self, new, specials=''):
        specials = [str(s) for s in specials] + list('x?')
        # check if overlapping data is equal
        overlap_comparison = compare_overlap(self.bib, new, specials)
        if not overlap_comparison.consistent:
            raise ValueError('Input DataFrame inconsistent with bibliography. Use compare_overlap to get mismatched rows.')
        # get portion of both dataframes eith overlapping data. These have:
        #     - columns whose labels are common to both frames
        #     - rows whose ref strings common to both frames
        bib_overlapping_rows, new_overlapping_rows = get_overlap(self.bib, new, specials=specials)
        # overwrite special character values in bib with corresponding new values
        is_special_value = lambda x: x in specials
        self.bib.update(new_overlapping_rows, filter_func=lambda x: Series(map(is_special_value, x)))
        # get all other rows in the new dataframe
        common_cols = bib_overlapping_rows.columns
        non_overlapping_rows = new[common_cols].loc[~new['ref'].isin(new_overlapping_rows['ref'])]
        # add unique new documents to the bibliography
        where_refs_unique = ~non_overlapping_rows['ref'].duplicated(keep=False)
        new_unique_documents = non_overlapping_rows.loc[where_refs_unique]
        self.bib = self.bib.append(new_unique_documents, ignore_index=True)
        # the remaining documents have ref strings that were not initially in the
        # bibliography and are repeated in multiple rows of the new dataframe
        remainder = non_overlapping_rows.loc[~where_refs_unique]
        common_cols = [col for col in remainder if col in self.bib]
        self.bib = self.bib.append(merge_rows(remainder[common_cols], specials=specials), ignore_index=True)
        self.bib = self.bib.fillna('x')


    def import_citations(self, new, specials='', cit_link_columns=['src','tgt']):
        if new.shape[0] == 0:
            print('Import citations got an input DataFrame with no rows')
            return

        specials = [str(s) for s in specials] + list('x?')
        new_link_columns = list(get_graph_link_labels(new))
        common_nonlink_cols = [c for c in new if ((c in self.cit) and (c not in cit_link_columns))]
        sorted_cit_cols = cit_link_columns + common_nonlink_cols
        select_new_cols = new_link_columns + common_nonlink_cols

        if self.cit.shape[0] == 0:
            not_special_value = lambda x: x not in specials
            has_src = new[new_link_columns[0]].apply(not_special_value)
            has_tgt = new[new_link_columns[1]].apply(not_special_value)
            where_complete_link = has_src & has_tgt
            sorted_new_data = new[select_new_cols].loc[where_complete_link]
            sorted_new_data.columns = sorted_cit_cols
            self.cit = sorted_new_data
            return

        is_special_value = lambda x: x in specials
        isnumber = lambda x: isinstance(x, Number)

        # check if all the source and target ids in both dataframes are all numeric or
        # all alphanumeric
        where_number_new = new[new_link_columns].applymap(isnumber)
        if (not where_number_new.all().all()) and where_number_new.any().any():
            raise ValueError('Link IDs must all be bibliography index values or all alphanumeric ref strings. Input DataFrame has some number and some non-number values in columns {}'.format(new_link_columns))
        where_number_cit = self.cit[['src', 'tgt']].applymap(isnumber)
        if where_number_cit.all().all():
            if not where_number_new.all().all():
                raise ValueError('Existing link IDs are all number values but input DataFrame contains non-number-valued link IDs in either or both columns {}'.format(new_link_columns))
        elif where_number_new.all().all():
            raise ValueError('Existing link IDs are all non-number values but input DataFrame contains number-valued link IDs in either or both columns {}'.format(new_link_columns))

        # check if new dataframe is modifying reference data for existing sources
        compare_link_endpoints, compare_link_data = compare_graph(self.cit, new, specials=specials)
        if not compare_link_endpoints.consistent:
            print('\nInput DataFrame contains sources which are listed in the existing citation graph but whose target sets differ.')
            print('Adding any new targets from the input DataFrame to the existing graph.\n')
        if not compare_link_data.consistent:
            raise ValueError('Input DataFrame inconsistent with existing citation graph. Use compare_graph to get mismatched rows.')

        cit_link_ids = self.cit.apply(lambda x: ' '.join(map(str, x[['src', 'tgt']])), axis=1)
        new_link_ids = new.apply(lambda x: ' '.join(map(str, x[new_link_columns])), axis=1)
        self.cit = self.cit[sorted_cit_cols]
        new = new[select_new_cols]
        new = new.loc[~new[new_link_columns[0]].apply(is_special_value)]
        new.columns = sorted_cit_cols
        cit_overlapping_rows, new_overlapping_rows = get_overlap(self.cit, new, specials=specials, unique_id=['src','tgt'])
        self.cit.update(new_overlapping_rows, filter_func=lambda x: Series(map(is_special_value, x)))
        overlapping_srcs = new['src'].isin(new_overlapping_rows['src'])
        overlapping_tgts = new['tgt'].isin(new_overlapping_rows['tgt'])
        non_overlapping_rows = new.loc[~(overlapping_srcs & overlapping_tgts)]
        where_ids_unique = ~non_overlapping_rows[['src', 'tgt']].duplicated(keep=False)
        new_unique_links = non_overlapping_rows.loc[where_ids_unique]
        self.cit = self.cit.append(new_unique_links, ignore_index=True)
        remainder = non_overlapping_rows.loc[~where_ids_unique]
        common_cols = [col for col in remainder if col in self.cit]
        merged_new_links = merge_rows(remainder[common_cols], specials=specials, unique_id=['src','tgt'])
        self.cit = self.cit.append(merged_new_links, ignore_index=True)


    def update_entry(self, entry, src=None, tgt=None, fillna='x'):
        '''
        Take data for a bibliography entry and either overwrite an
        existing entry with new data, rewrite the same entry, or add
        a new entry to the bibliography.

        Parameters
        ----------
        entry : pd.Series or pd.DataFrame
            either a pandas Series object or a DataFrame containing a
            single row that can be squeezed into a Series. Contains
            data for a bibliography entry that may or may not already
            exist in the bibliography DataFrame.

        src : integer
            Bibliography index of source that references this target.
        '''
        result = get_updated_entry(self.bib, entry)

        if result.new:
            entry = Series(result.entry, index=self.bib.columns).fillna(fillna)
            self.bib = self.bib.append(entry, ignore_index=True)

        elif result.updated:
            if len(self.bib) == 0:
                if result.index is None:
                    entry = Series(result.entry, index=self.bib.columns).fillna(fillna)
                    self.bib = self.bib.append(entry, ignore_index=True)
                else:
                    raise ValueError('update_entry got an empty bibliography but the index to be updated is not None')
            else:
                self.bib.loc[result.index] = result.entry

        if (src is not None) and (tgt is None):
            tgt = entry.ref
            if isinstance(src, Number):
                src = self.bib.loc[src, 'ref']
                if not (self.cit[['src', 'tgt']] == [src, tgt]).all(axis=1).any():
                    self.cit = self.cit.append({'src':src, 'tgt':tgt}, ignore_index=True).fillna(fillna)
            elif ' ' in src:
                if not (self.bib['ref'] == src).any():
                    raise ValueError('src ref string "{}" not in bibliography'.format(src))
                if not (self.cit[['src', 'tgt']] == [src, tgt]).all(axis=1).any():
                    self.cit = self.cit.append({'src':src, 'tgt':tgt}, ignore_index=True).fillna(fillna)
            else:
                raise ValueError('src is not a number and contains no spaces (cannot be an index or a ref string)')

        elif (src is None) and (tgt is not None):
            src = entry.ref
            if isinstance(tgt, Number):
                tgt = self.bib.loc[tgt, 'ref']
                if not (self.cit[['src', 'tgt']] == [src, tgt]).all(axis=1).any():
                    self.cit = self.cit.append({'src':src, 'tgt':tgt}, ignore_index=True).fillna(fillna)
            elif ' ' in tgt:
                if not (self.bib['ref'] == tgt).any():
                    raise ValueError('tgt ref string "{}" not in bibliography'.format(tgt))
                if not (self.cit[['src', 'tgt']] == [src, tgt]).all(axis=1).any():
                    self.cit = self.cit.append({'src':src, 'tgt':tgt}, ignore_index=True).fillna(fillna)
            else:
                raise ValueError('tgt is not a number and has no spaces (cannot be an index or a ref string)')

        elif (src is not None) and (tgt is not None):
            raise ValueError('CitNet.update_entry accepts a value for src or tgt, not both')
