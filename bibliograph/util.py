from networkx import DiGraph
from networkx import Graph
from numbers import Number
from os.path import isfile
from pandas import DataFrame
from pandas import Series
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
        g = DiGraph()
    else:
        g = Graph()

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
    if type(entry) is not Series:
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
            raise ValueError('Found repeated ref strings in bibliography when processing\n' + str(entry))
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
    return Series(dict(zip(ref_cols, bib_values)))

class ManualQuoteError(Exception):
    # Constructor method
    def __init__(self, line_num='???', num_quotes='???', fname='???'):
        self.line_num = str(line_num)
        self.num_quotes = str(num_quotes)
        self.fname = str(fname)
        self.value = self.make_message()
    # __str__ display function
    def __str__(self):
        return(repr(self.value))

    def make_message(self):
        return 'Line {} in file {} has {} double quote(s). Must be 0 or 2.'.format(self.line_num, self.fname, self.num_quotes)

    def set_line_num(self, line_num):
        self.line_num = str(line_num)
        self.value = self.make_message()

    def set_num_quotes(self, num_quotes):
        self.num_quotes = str(num_quotes)
        self.value = self.make_message()

    def set_fname(self, fname):
        self.fname = str(fname)
        self.value = self.make_message()

class ManualLineError(Exception):
    # Constructor method
    def __init__(self, line_num='???', fname='???'):
        self.line_num = str(line_num)
        self.fname = str(fname)
        self.value = self.make_message()
    # __str__ display function
    def __str__(self):
        return(repr(self.value))

    def make_message(self):
        return 'first and last characters in line {} in file {} must be commas'.format(self.line_num, self.fname)

    def set_line_num(self, line_num):
        self.line_num = str(line_num)
        self.value = self.make_message()

    def set_fname(self, fname):
        self.fname = str(fname)
        self.value = self.make_message()


def check_orphan(doc_id, bib, cit):
    if ' ' in str(doc_id):
        if (bib['ref'] == src).any() and (x not in cit['src']) and (x not in cit['tgt']):
            return True
        else:
            return False
    elif isinstance(doc_id, Number):
        if (x in bib.index) and (x not in cit['src']) and (x not in cit['tgt']):
            return True
        else:
            return False
    else:
        raise ValueError('doc_id is not a number and contains no spaces (cannot be an index or a ref string)')


def count_true(series):
    try:
        return series.value_counts()[True]
    except:
        return 0


class comparison_result:

    def __init__(self, consistent, row_set1, row_set2):
        self.consistent = consistent
        self.rows1 = row_set1
        self.rows2 = row_set2


def get_overlap(df1, df2, specials='', unique_id='ref'):
    if type(unique_id) != str:
        if len(unique_id) > 2:
            raise ValueError('cannot construct unique identifiers from more than two columns')
        df1_uniques = df1.apply(lambda x: ' '.join(map(str, x)), axis=1)
        df2_uniques = df2.apply(lambda x: ' '.join(map(str, x)), axis=1)
    else:
        df1_uniques = df1[unique_id]
        df2_uniques = df2[unique_id]
    common_cols = [col for col in df1 if col in df2]
    df1_common_uniques = df1_uniques.isin(df2_uniques)
    df2_common_uniques = df2_uniques.isin(df1_uniques)
    df1_overlapping_rows = df1[common_cols].loc[df1_common_uniques].drop_duplicates()
    df2_overlapping_rows = df2[common_cols].loc[df2_common_uniques].drop_duplicates()
    return df1_overlapping_rows, df2_overlapping_rows


def check_column_consistency(series, specials=''):
    counts = series.value_counts()
    nonspecial = [v for v in counts.index if str(v) not in specials]
    if len(nonspecial) not in [1, 0]:
        return False
    else:
        return True


def get_nonspecial_values(series, specials=''):
    counts = series.value_counts()
    return [v for v in counts.index if str(v) not in specials]


def get_inconsistent_ids(df, specials='', unique_id='ref'):
    where_nonunique_ids = df[unique_id].duplicated(keep=False)
    if not any(where_nonunique_ids):
        return []
    else:
        rows_with_nonunique_ids = df.loc[where_nonunique_ids]
        check_group = lambda x: all(x.apply(check_column_consistency, args=(specials,)))
        consistent_ids = rows_with_nonunique_ids.groupby(unique_id).apply(check_group)
        return [value for value in consistent_ids.index if not consistent_ids[value]]


def merge_rows(df, specials='', unique_id='ref', fill='x'):
    multi_id = False
    if type(unique_id) != str:
        if len(unique_id) > 2:
            raise ValueError('cannot construct unique identifiers from more than two columns')
        multi_id = True
        df.insert(0, 'id', df[unique_id].apply(lambda x: ' '.join(map(str, x)), axis=1))
        unique_id = 'id'
    inconsistent_ids = get_inconsistent_ids(df, specials=specials, unique_id=unique_id)
    if len(inconsistent_ids) > 0:
        raise ValueError('found the following ref strings with inconsistent rows', inconsistent_ids)
    rows_with_nonunique_ids = df.loc[df[unique_id].duplicated(keep=False)]
    unique_nonspecial_value = lambda x: (get_nonspecial_values(x, specials) or [fill])[0]
    get_group_values = lambda x: x.apply(unique_nonspecial_value)
    values_by_id = rows_with_nonunique_ids.groupby(unique_id).apply(get_group_values)
    rows_with_unique_ids = df.loc[~df[unique_id].duplicated(keep=False)]
    if len(values_by_id) == 0:
        if multi_id:
            return DataFrame(columns=df.columns[1:])
        else:
            return DataFrame(columns=df.columns)
    else:
        new_df = DataFrame(data=values_by_id.values, columns=df.columns)
        new_df = rows_with_unique_ids.append(new_df)
        if multi_id:
            return new_df[new_df.columns[1:]]
        else:
            return new_df


def compare_overlap(df1, df2, specials='', unique_id='ref'):
    df1_overlapping_rows, df2_overlapping_rows = get_overlap(df1, df2, specials=specials, unique_id=unique_id)

    if any(df1_overlapping_rows[unique_id].duplicated()):
        if any(df2_overlapping_rows[unique_id].duplicated()):
            raise ValueError('got unique_id={} but both dataframes have duplicate values in that column.'.format(unique_id))
        raise ValueError('got unique_id={} but first dataframe has duplicate values in that column.'.format(unique_id))
    if any(df2_overlapping_rows[unique_id].duplicated()):
        raise ValueError('got unique_id={} but second dataframe has duplicate values in that column.'.format(unique_id))

    df1_overlapping_rows = df1_overlapping_rows.sort_values(by=[unique_id])
    df2_overlapping_rows = df2_overlapping_rows.sort_values(by=[unique_id])
    not_special = lambda x: str(x) not in specials
    df1_has_data = df1_overlapping_rows.applymap(not_special)
    df2_has_data = df2_overlapping_rows.applymap(not_special)
    both_have_data = df1_has_data.values & df2_has_data.values
    df1_data_overlap = df1_overlapping_rows.where(both_have_data).fillna('x')
    df2_data_overlap = df2_overlapping_rows.where(both_have_data).fillna('x')
    where_consistent = df1_data_overlap.values == df2_data_overlap.values
    everywhere_consistent = where_consistent.all()
    df1_mismatched_rows = df1_overlapping_rows.loc[~where_consistent.all(axis=1)]
    df2_mismatched_rows = df2_overlapping_rows.loc[~where_consistent.all(axis=1)]
    return comparison_result(everywhere_consistent, df1_mismatched_rows, df2_mismatched_rows)


def get_graph_link_labels(df):
    cols = list(df.columns)

    possibilities = ['src', 'tgt', 'ref']
    counts = [cols.count(end) for end in possibilities]
    total = sum(counts)
    if total > 3:
        raise ValueError('Input DataFrame must have at most three columns whose labels are in', possibilities)
    if total < 2:
        raise ValueError('Input DataFrame must have at least two columns whose labels are in', possibilities)
    duplicated = [possibilities[i] for i,v in enumerate(counts) if v > 1]
    if duplicated:
        raise ValueError('Input DataFrame has duplicated column label(s) {}. Must have at most one of each.'.format(str(duplicated)))

    if 'src' in cols:
        if 'tgt' in cols:
            return 'src', 'tgt'
        elif 'ref' in cols:
            return 'src', 'ref'
    elif 'tgt' in cols:
        if 'ref' in cols:
            return 'ref', 'tgt'


def compare_graph(df1, df2, specials=''):
    src_lbl1, tgt_lbl1 = get_graph_link_labels(df1)
    src_lbl2, tgt_lbl2 = get_graph_link_labels(df2)

    not_special = lambda x: x not in specials

    if ~df1[src_lbl1].isin(df2[src_lbl2]).any():
        no_overlap_result = comparison_result(True, DataFrame([]),DataFrame([]))
        return no_overlap_result, no_overlap_result

    # if a source is present in both dataframes, check if df1 has extra targets
    # not linked in df2
    df1_tgt_is_in_df2 = lambda x: x[tgt_lbl1].isin(df2[tgt_lbl2]).all()
    df1_srcs_consistent = df1.groupby(src_lbl1).apply(df1_tgt_is_in_df2)
    df1_srcs_consistent = df1_srcs_consistent.drop([v for v in specials if v in df1_srcs_consistent])
    df1_inconsistent_srcs = df1_srcs_consistent[df1_srcs_consistent==False].index
    df1_inconsistent_rows = df1.loc[df1[src_lbl1].isin(df1_inconsistent_srcs)]
    # if a source is present in both dataframes, check if df2 has extra targets
    # not linked in df1
    df2_tgt_is_in_df1 = lambda x: x[tgt_lbl2].isin(df1[tgt_lbl1]).all()
    df2_srcs_consistent = df2.groupby(src_lbl2).apply(df2_tgt_is_in_df1)
    df2_srcs_consistent = df2_srcs_consistent.drop([v for v in specials if v in df2_srcs_consistent])
    df2_inconsistent_srcs = df2_srcs_consistent[df2_srcs_consistent==False].index
    df2_inconsistent_rows = df2.loc[df2[src_lbl2].isin(df2_inconsistent_srcs)]

    if not all(df1_srcs_consistent) or not all(df2_srcs_consistent):
        tgt_sets_consistent = False
    else:
        tgt_sets_consistent = True
    tgt_set_result = comparison_result(True, df1_inconsistent_rows, df2_inconsistent_rows)

    df1_link_ids = df1.apply(lambda x: ' '.join(map(str, x[[src_lbl1, tgt_lbl1]])), axis=1)
    df2_link_ids = df2.apply(lambda x: ' '.join(map(str, x[[src_lbl2, tgt_lbl2]])), axis=1)
    df1.insert(0, 'link_ID', df1_link_ids)
    df2.insert(0, 'link_ID', df2_link_ids)
    try:
        where_df1_links_common = df1_link_ids.isin(df2_link_ids)
        where_df2_links_common = df2_link_ids.isin(df1_link_ids)
        df1_common_rows = df1.loc[where_df1_links_common]
        df2_common_rows = df2.loc[where_df2_links_common]
        data_result = compare_overlap(df1_common_rows, df2_common_rows, specials=specials, unique_id='link_ID')
    finally:
        del df1['link_ID']
        del df2['link_ID']

    return tgt_set_result, data_result
