import pandas as pd
import networkx as nx
from os.path import isfile
from shutil import copyfile

def backup(filename):
    '''
    take a filename and create the file or backup an existing file as
    filename.bakX, where X is a sequential integer.

    Parameters
    ----------
    filename : string
        Name of file

    Returns
    -------
    nothing
    '''
    backup = filename + '.bak0'
    if isfile(filename):
        while isfile(backup):
            backup = backup.split('.bak')
            backup = backup[0] + '.bak' + str(int(backup[1]) + 1)
        copyfile(filename, backup)

def makeGraph(nodes, edges, directed=True):
    '''
    TODO : allow columns that contain additional edge data

    Create a NetworkX graph from pandas DataFrames representing the
    nodes and edges of the graph. 'ref' is a label for a column in the
    nodes DataFrame that contains a unique identifier for each row.
    The values in the column labeled by 'ref' become nodes in the graph.
    Values in all other nodes columns are stored as node data.

    Parameters
    ----------
    nodes : pd.DataFrame
        DataFrame of nodes. Must contain a column other than the index
        whose values are unique for each row.

    edges : pd.DataFrame
        DataFrame of edges. Contains at least a column labeled 'src'
        and a columne labeled 'tgt'
    directed : boolean
        Optional flag to create a directed or undirected NetworkX
        graph. Default is True.

    Returns
    -------
    g : nx.DiGraph or nx.Graph
        A NetworkX graph object, either DiGraph or Graph depending on
        the value of the directed input parameter.
    '''

    if directed:
        g = nx.DiGraph()
    else:
        g = nx.Graph()

    for i in nodes.index:
        entry = nodes.loc[i].squeeze()
        g.add_node(entry['ref'])
        for c in [c for c in nodes.columns if c != 'ref']:
            g.nodes[entry['ref']][c] = entry[c]

    for i in edges.index:
        g.add_edge(edges.loc[i, 'src'], edges.loc[i, 'tgt'])

    return(g)

class updateResult:
    '''
    Object to conveniently store data for a bibliography update
    operation.

    Attributes
    ----------
    new : boolean
        True if the bibliography entry should be appended to the
        bibliography. False if the result should modify fields in an
        existing bibliography entry.

    updated : boolean
        True if the bibliography entry should overwrite an existing
        entry.

    entry : pd.Series
        pandas series object representing a new entry or an entry that
        should be overwritten.

    index : integer (probably)
        Contains the name of the pandas series stored in entry.
        Likely an integer corresponding to the bibliography index for
        the row to be added or overwritten, but this could vary if
        bibliography is indexed by something other than integers.
    '''
    def __init__(self, new, updated, entry=None, index=None):
        self.new = new
        self.updated = updated
        self.entry = entry
        self.index = index

def get_updated_entry(bib, entry, specials='x?'):
    '''
    Check if a ref string exists in the bib DataFrame. If yes, check
    if entry contains values for bib fields which are 'x' in the
    existing DataFrame and update fields as necessary. If the ref
    string does not exist, return the new bibliography entry
    unmodified.

    Parameters
    ----------
    bib : pd.DataFrame
        pandas DataFrame containing bibliography data

    entry : pd.Series or pd.DataFrame
        either a pandas Series object or a DataFrame containing a
        single row that can be squeezed into a Series. Contains data
        for a bibliography entry that may or may not already exist in
        the bibliography DataFrame.

    Returns
    -------
    updateResult
        A bibliograph.util.updateResult object containing a new
        bibliography entry or an updated entry.
    '''
    if type(entry) is not pd.Series:
        if len(entry) == 1:
            entry = entry.squeeze()
        else:
            raise ValueError('entry must be pd.Series or pd.DataFrame-like object with a squeeze method and exactly one row.')

    if (bib['ref'] == entry['ref']).any():
        to_update = bib.loc[bib['ref'] == entry['ref']].copy()
        if len(to_update) == 1:
            to_update = to_update.squeeze()
            updated = False
            for c in entry.index:
                if (str(to_update[c]) in specials) and (entry[c] not in specials):
                    to_update[c] = entry[c]
                    updated = True
            return(updateResult(False, updated, entry=to_update, index=to_update.name))
        else:
            raise RuntimeError('Found repeated ref strings in bibliography when processing\n' + str(entry))
    else:
        return(updateResult(True, False, entry=entry))


def df_progress_bar(tot, pos, prefix = '', suffix = '', decimals = 1, length = 50, fill = '█', printEnd = "\r"):

    percent = ("{0:." + str(decimals) + "f}").format(100 * (pos / float(tot)))
    filledLength = int(length * pos // tot)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)


def make_ref_str(doc, ref_cols, special='x'):
    d = {k:v for k,v in doc.items()}
    [d.update({c:special}) for c in ref_cols if c not in d.keys()]
    [d.update({c:''.join(d[c].split())}) for c in d.keys() if (' ' in d[c])]
    return ' '.join([str(d[c]) for c in ref_cols])


def add_ref_to_dataframe(df, ref_cols, special='x'):
    new_df = df.copy()
    new_df.insert(len(df.columns), 'ref', ['x']*len(df))
    new_df['ref'] = new_df.apply((lambda x: make_ref_str(x, ref_cols, special)), axis=1)
    return new_df


def rawcount(filename):
    with open(filename, 'rb') as f:
        lines = 0
        buf_size = 1024 * 1024
        read_f = f.raw.read

        buf = read_f(buf_size)
        while buf:
            lines += buf.count(b'\n')
            buf = read_f(buf_size)

    return lines


def expand_ref_str(ref_str, ref_cols):
    bib_values = ref_str.split()
    return pd.Series(dict(zip(ref_cols, bib_values)))
