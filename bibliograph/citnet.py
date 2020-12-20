import pandas as pd
import networkx as nx
from .nasaads import queryads
from .nasaads import query_ads_bibcodes
from .readwrite import slurp_bibtex
from .readwrite import slurp_csv
from .util import backup
from .util import updater


class CitNet:
    '''
    The citnet class provides a citation network object that contains
    a pandas DataFrame for a bibliography, a DataFrame for references
    between bibliography, and a NetworkX graph representing the
    network.

    Parameters
    ----------
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

    def __init__(self, **kwargs):

        if (('refcols' not in kwargs) or ('bibcols' not in kwargs)) and ('bibtex' in kwargs):
            raise ValueError('bibcols and refcols required when initializing from bibtex.')
        if ('bibcols' in kwargs):
            bibcols = kwargs['bibcols']
            if 'ref' not in bibcols:
                bibcols = bibcols + ['ref']

        dfargs = ['data', 'index', 'columns']
        self.bib = pd.DataFrame(dtype='str', **{arg:kwargs[arg] for arg in dfargs if arg in kwargs})
        if 'citcols' in kwargs:
            self.cit = pd.DataFrame(columns=(['src', 'tgt'] + kwargs['citcols']))
        else:
            self.cit = pd.DataFrame(columns=['src', 'tgt'])

        if 'bibtex' in kwargs:
            if ('csv' in kwargs) or ('fileprefix' in kwargs):
                raise ValueError('citnet is initialized with exactly one of bibtex, csv, or fileprefix. Got values for at least two.')
            bibtex = kwargs['bibtex']
            if 'refcols' in kwargs:
                self.refcols = kwargs['refcols']
            else:
                self.refcols = 'title'
            bibtexargs = ['bibcols', 'bibtex_parsers']
            print('\nLoading data from ' + bibtex + '\n')
            slurp_bibtex(self, bibtex, self.refcols, **{arg:kwargs[arg] for arg in bibtexargs if arg in kwargs})
            self.bib = self.bib.fillna('x')

        if 'csv' in kwargs:
            if ('bibtex' in kwargs) or ('fileprefix' in kwargs):
                raise ValueError('citnet is initialized with exactly one of bibtex, csv, or fileprefix. Got values for at least two.')
            csv = kwargs['csv']
            csvargs = ['direction', 'sources_from_csv', 'csv_separator', 'csv_parser']
            print('\nLoading data from ' + csv + '\n')
            slurp_csv(self, csv, **{arg:kwargs[arg] for arg in csvargs if arg in kwargs})

        if 'fileprefix' in kwargs:
            if ('bibtex' in kwargs) or ('csv' in kwargs):
                raise ValueError('citnet is initialized with exactly one of bibtex, csv, or fileprefix. Got values for at least two.')
            fileprefix = kwargs['fileprefix']
            self.bib = pd.read_json(fileprefix + '-bib.json')
            try:
                self.cit = pd.read_json(fileprefix + '-cit.json')
            except FileNotFoundError:
                print(fileprefix + '-cit.json not found, creating blank citation dataframe.')
            try:
                self.graph = nx.read_graphml(fileprefix + '.graphml')
            except FileNotFoundError:
                print(fileprefix + '.graphml not found, creating blank graph.')
                print('NOT IMPLEMENTED')

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
        Wraps pd.DataFrame.append() so append on CitNet appends to the
        bib DataFrame.
        '''
        self.bib = self.bib.append(*args, **kwargs)

    def update_entry(self, entry, update_citations=False, src=None):
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
        result = updater(self.bib, entry)

        if result.updated:
            self.bib.loc[result.index] = result.entry
            if update_citations and not ((self.cit.src == src) & (self.cit.tgt == result.index)).any():
                self.cit = self.cit.append({'src':src, 'tgt':self.bib.index[-1]}, ignore_index=True)

    def load_reference_csv(self, csv, **kwargs):
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

    def get_ads_bibcodes(self, search_cols, **kwargs):
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

    def queryads(self, search_cols, fetch_terms, **kwargs):
        '''
        Get ADS data for bibliography entries.

        Parameters
        ----------
        search_cols : list-like
            Column labels in the sources DataFrame which contain data
            for the ADS search queries.

        fetch_terms : list-like
            ADS search terms to get for each query. This is a list of
            data to fields to download from the API rather than search
            terms with which to construct the search query from the
            sources DataFrame. List of ADS terms is in the drop-down
            menu above the search bar at
            https://ui.adsabs.harvard.edu/

        kwargs
            Keyword arguments are passed directly to
            bibliograph.nasaads.queryADSbibcodes

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

        results, queries, badqueries = queryads(self.bib, search_cols, fetch_terms, **kwargs)

        this_updater = (lambda x: self.update_entry(x, update_citations=True, src=x['srcidx']))

        results[results.columns[:-1]].apply(this_updater, axis=1)

        return(results, queries, badqueries)

