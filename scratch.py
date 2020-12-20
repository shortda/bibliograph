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
            self.graph = makeGraph(self)

        if csv is not None:

            if (bibtex is not None) or (fileprefix is not None):

                raise ValueError('citnet is initialized with exactly one of bibtex, csv, or fileprefix. Got at values for at least two.')

            print('\nLoading data from ' + csv + '\n')
            slurpReferenceCSV(self, csv, direction, sources_from_csv, csv_separator, csv_parser)

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

cols = ['A', 'B', 'C']
new_property = cols[:2]
tdf = thisDF(columns=cols, new_property=new_property)

print(tdf.new_property)

tdf.append(pd.Series(['a', 'b', 'c'], index=tdf.columns), ignore_index=True)

print(tdf.new_property)