import pandas as pd
import networkx as nx


class CitNet:

    def __init__(self, bibcols=None, refcols=None, citcols=None, data=None, index=None, bibtex=None, csv=None, fileprefix=None, bibtex_parsers=None, direction='outgoing', sources_from_csv=True, csv_separator=' | ', csv_parser=None):

        if 'ref' not in bibcols:
            bibcols = bibcols + ['ref']
        if (refcols is None) and (bibcols is None) and (bibtex is not None):
            raise ValueError('bibcols and refcols required when initializing from bibtex.')

        self.bib = pd.DataFrame(data=data, index=index, columns=bibcols, dtype='str')
        self.cit = pd.DataFrame(columns=(['src', 'tgt'] + citcols), dtype='int')

        if bibtex is not None:
            if (csv is not None) or (fileprefix is not None):
                raise ValueError('citnet is initialized with exactly one of bibtex, csv, or fileprefix. Got at values for at least two.')
            self.refcols = refcols
            print('\nLoading data from ' + bibtex + '\n')
            slurp_bibtex(self, bibtex, bibcols=bibcols, refcols=refcols, tag_processors=bibtex_parsers)
            self.bib = self.bib.fillna('x')

        if csv is not None:
            if (bibtex is not None) or (fileprefix is not None):
                raise ValueError('citnet is initialized with exactly one of bibtex, csv, or fileprefix. Got at values for at least two.')
            print('\nLoading data from ' + csv + '\n')
            slurp_csv(self, csv, direction, sources_from_csv, csv_separator, csv_parser)

        if fileprefix is not None:
            if (bibtex is not None) or (csv is not None):
                raise ValueError('citnet is initialized with exactly one of bibtex, csv, or fileprefix. Got at values for at least two.')
            self.bib = pd.read_json(fileprefix + '-bib.json')
            try:
                self.cit = pd.read_json(fileprefix + '-cit.json')
            except FileNotFoundError:
                print(fileprefix + '-cit.json not found, creating blank citation dataframe.')
                if citcols is not None:
                    self.cit = pd.DataFrame(columns=(['src', 'tgt'] + citcols))
                else:
                    self.cit = pd.DataFrame(columns=['src', 'tgt'])
            try:
                self.graph = nx.read_graphml(fileprefix + '.graphml')
            except FileNotFoundError:
                print(fileprefix + '.graphml not found, creating blank citation dataframe.')
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

        self.bib = self.bib.append(*args, **kwargs)

    def update_entry(self, entry, update_citations=False, src=None):

        result = updater(self.bib, entry)

        if result.updated:
            self.bib.loc[result.index] = result.entry
            if update_citations and not ((self.cit.src == src) & (self.cit.tgt == result.index)).any():
                self.cit = self.cit.append({'src':src, 'tgt':self.bib.index[-1]}, ignore_index=True)

    def load_reference_csv(self, csv, **kwargs):

        print('\nLoading data from ' + csv + '\n')
        slurp_csv(self, csv, **kwargs)

    def save_citnet(self, name):

        backup(name + '-bib.json')
        self.bib.to_json(name + '-bib.json')

        backup(name + '-cit.json')
        self.bib.to_json(name + '-cit.json')

        if self.graph:
            backup(name + '.graphml')
            nx.write_graphml(self.graph, name + '.graphml')

    def get_ads_bibcodes(self, search_cols, **kwargs):

        queries, badqueries = query_ads_bibcodes(self.bib, search_cols, **kwargs):

        self.bib.loc[queries.index, 'bibcode'] = queries['bibcode']
        self.bib.loc[badqueries, 'bibcode'] = '?'

        return(queries, badqueries)

    def queryads(self, search_cols, fetch_terms, **kwargs):

        results, queries, badqueries = queryads(self.bib, search_cols, fetch_terms, **kwargs)

        this_updater = lambda x: self.update_entry(x, update_citations, x['srcidx'])

        results[results.columns[:-1]].apply(this_updater, axis=1)

        return(results, queries, badqueries)

