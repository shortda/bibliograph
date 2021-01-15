import pandas as pd
import networkx as nx
from .nasaads import query_ads
from .nasaads import query_bibcodes
from .readwrite import slurp_bibtex
from .readwrite import slurp_csv
from .util import backup
from .util import get_updated_entry


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

    def __init__(self, bibtex_params=None, csv_params=None, df_params=None, file_params=None):

        if [(bibtex_params is None), (csv_params is None), (df_params is None), (file_params is None)].count(False) > 1:
            raise ValueError('CitNet can only be initialized with exactly one of bibtex_params, csv_params, file_params, or df_params')

        if bibtex_params is not None:

            bibcols = bibtex_params['bibcols']
            if 'ref' not in bibcols:
                bibcols.append['ref']
            if 'refcols' in bibtex_params:
                self.refcols = bibtex_params['refcols']
            else:
                self.refcols = 'title'
            self.bib = pd.DataFrame(columns=bibcols)

            fname = bibtex_params['fname']
            print('\nLoading data from ' + fname + '\n')
            bibtexargs = ['bibcols', 'bibtex_parsers']
            slurp_bibtex(self, fname, self.refcols, **{arg:bibtex_params[arg] for arg in bibtexargs if arg in bibtex_params})

            self.bib = self.bib.fillna('x')

        elif csv_params is not None:

            fname = csv_params['fname']
            csvargs = ['direction', 'sources_from_csv', 'csv_separator', 'csv_parser']
            print('\nLoading data from ' + fname + '\n')
            slurp_csv(self, fname, **{arg:csv_params[arg] for arg in csvargs if arg in csv_params})

        elif df_params is not None:

            self.bib = pd.DataFrame(dtype=str, **df_params)

        elif file_params is not None:

            prefix = file_params['prefix']
            self.bib = pd.read_json(prefix + '-bib.json')
            try:
                self.cit = pd.read_json(prefix + '-cit.json')
            except FileNotFoundError:
                print(prefix + '-cit.json not found, creating blank citation dataframe.')
            try:
                self.graph = nx.read_graphml(prefix + '.graphml')
            except FileNotFoundError:
                print(prefix + '.graphml not found, creating blank graph.')
                print('NOT IMPLEMENTED')

        else:

            self.bib = pd.DataFrame()

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

    def update_entry(self, entry, update_citations=False, src=None, fillna='x'):
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

        update_citations : boolean
            If True, create new citation edge for this target.

        src : integer
            Bibliography index of source that references this target.
        '''
        result = get_updated_entry(self.bib, entry)

        if result.new:

            entry = pd.Series(result.entry, index=self.bib.columns).fillna(fillna)
            self.bib = self.bib.append(entry, ignore_index=True)

        elif result.updated:

            if len(self.bib) == 0:
                if result.index is None:
                    entry = pd.Series(result.entry, index=self.bib.columns).fillna(fillna)
                    self.bib = self.bib.append(entry, ignore_index=True)
                else:
                    raise ValueError('update_entry got an empty bibliography but the index to be updated is not None')
            else:
                self.bib.loc[result.index] = result.entry

        if update_citations and (src is not None):
            if not (self.cit[['src', 'tgt']] == [src, result.index]).any().all():
                if result.new:
                    self.cit = self.cit.append({'src':src, 'tgt':self.bib.index[-1]}, ignore_index=True)
                else:
                    self.cit = self.cit.append({'src':src, 'tgt':result.index})

        elif update_citations and (src is None):

            raise ValueError('update_entry got update_citations=True but no source index.')


    def load_csv(self, csv, **kwargs):
        '''
        Get bibliography and citation data from a csv file.

        Parameters
        ----------
        csv : string
            Name of csv file.

        kwargs
            Keyword arguments passed to
            bibliography.readwrite.slurp_csv
        '''
        print('\nLoading data from ' + csv + '\n')
        slurp_csv(self, csv, **kwargs)

    def save_citnet(self, name):
        '''
        Write JSON files representing the bibliography and citation
        DataFrames. Write a graphml file representing the graph.

        Parameters
        ----------
        name : string
            Network name will be the prefix for all stored filenames.
        '''

        backup(name + '-bib.json')
        self.bib.to_json(name + '-bib.json')
        backup(name + '-cit.json')
        self.bib.to_json(name + '-cit.json')

        if self.graph:
            backup(name + '.graphml')
            nx.write_graphml(self.graph, name + '.graphml')

    def get_bibcodes(self, search_cols, **kwargs):
        '''
        Get ADS bibcodes for papers in the sources DataFrame. If this
        function tries to construct a query for some entry but can't
        (the index for that entry winds up in badQueries), the
        bibcode field for that entry gets '?'.

        Parameters
        ----------
        search_cols : list-like
            Column labels in the sources DataFrame which contain data
            for the ADS search queries.

        kwargs
            Keyword arguments are passed directly to
            bibliograph.nasaads.query_ads_bibcodes

        Returns
        -------
        queries : pd.DataFrame
            pandas DataFrame with query strings, ads articles objects
            retreived from the ADS, and bibcodes from those articles
            objects. queries index corresponds to sources index.

        badqueries : list
            List of index values from the sources DataFrame for which
            values in columns to be searched either contained spaces
            or were 'x'.
        '''

        queries, badqueries = query_ads_bibcodes(self.bib, search_cols, **kwargs)
        self.bib.loc[queries.index, 'bibcode'] = queries['bibcode']
        self.bib.loc[badqueries, 'bibcode'] = '?'
        return(queries, badqueries)

    def get_ads_data(self, search_cols, fetch_cols, **kwargs):
        '''
        Get ADS data for bibliography entries.

        Parameters
        ----------
        search_cols : list-like
            Column labels in the sources DataFrame which contain data with
            which to construct the ADS search queries.

        fetch_cols : list-like
            Column labels in the sources DataFrame that should be populated
            with data from ADS.

        kwargs
            Keyword arguments are passed directly to
            bibliograph.nasaads.query_ads

        Returns
        -------
        results : pd.DataFrame
            pandas DataFrame with retrieved data. Rrsults index will
            be the bibliography index or query_mask index if there is
            no wrapper keyword. If there is a wrapper keyword such as
            'citations' or 'references', results may be longer than
            the bibliography or query_index.

        queries : pd.DataFrame
            pandas DataFrame with query strings and ads articles
            objects retreived from the ADS. queries index is either
            the bibliography index or the index of query_mask.
        '''

        results, queries = query_ads(self.bib, search_cols, fetch_cols, **kwargs)
        this_updater = (lambda x: self.update_entry(x, update_citations=True, src=x['srcidx']))
        results[results.columns[:-1]].apply(this_updater, axis=1)
        return(results, queries)
