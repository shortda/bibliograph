import pandas as pd
import networkx as nx
from os.path import isfile
from shutil import copyfile

def getBibtexTags(bibtex, skiptags=[]):
	'''
	Scan a bibtex file and return a list of all BibTex tags used in
	the file.

	Parameters
	----------
	bibtex : string
		The name of a bibtex file to scan

	skiptags : list
		Optional list of tags to ignore

	Returns
	-------
	tags : list
		List of tag strings found in the BibTex file
	'''

	with open(bibtex, encoding='utf8') as infile:
		tags = []
		for entry in infile.read().split('@'):
			entry = entry.translate(str.maketrans('','','{}\t')).split(',\n')
			for line in entry[1:]:
				tag = line.split('=')[0].strip()
				if (tag not in tags) and (tag not in skiptags):
					tags.append(tag)
	return(tags)

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

def makeGraph(nodes, edges, uid, directed=True):
	'''
	TODO : allow edges columns that contain additional edge data

	Create a NetworkX graph from pandas DataFrames representing the
	nodes and edges of the graph. uid is a label for a column in the 
	nodes DataFrame that contains a unique identifier for each row. 
	The values in the column labeled by uid become nodes in the graph.
	Values in all other nodes columns are stored as node data.

	Parameters
	----------
	nodes : pd.DataFrame
		DataFrame of nodes. Must contain a column other than the index
		whose values are unique for each row.

	edges : pd.DataFrame
		DataFrame of edges. Contains at least a column labeled 'src'
		and a columne labeled 'tgt'

	uid : string (probably)
		Label of the column containing unique identifiers for each row

	directed : boolean
		Optional flag to create a directed or undirected NetworkX
		graph. Default is True.

	Returns
	-------
	g : nx.DiGraph or nx.Graph
		A NetworkX graph object, either DiGraph or Graph depending on
		the value of the directed input parameter.
	'''
	if uid not in nodes.columns:
		raise ValueError('uid must be a column in the nodes DataFrame')

	if directed:
		g = nx.DiGraph()
	else:
		g = nx.Graph()

	for i in nodes.index:
		entry = nodes.loc[i].squeeze()
		g.add_node(entry[uid])
		for c in [c for c in nodes.columns if c is not uid]:
			g.nodes[entry[uid]][c] = entry[c]

	for i in edges.index:
		g.add_edge(edges.loc[i, 'src'], edges.loc[i, 'tgt'])

	return(g)

class updateResult:
	'''
	Object to conveniently store data for a bibliography update
	operation.

	Attributes
	----------
	updated : boolean
		True if the bibliography update should modify fields in an
		existing bibliography entry. False if newEntry should be 
		appended to bibliography.

	entry : pd.Series
		pandas series object representing a new entry or an entry that
		should be overwritten.

	index : integer (probably)
		Contains the name of the pandas series stored in entry.
		Likely an integer corresponding to the bibliography index for
		the row to be added or overwritten, but this could vary if
		bibliography is indexed by something other than integers.
	'''
	def __init__(updated, entry=None, index=None):
		self.updated = updated
		if updated:
			self.entry = entry
			self.index = index

def bibUpdate(bib, newEntry, uid):
	'''
	Check if a unique identifier exists in the bib DataFrame. If the 
	uid exists, check if newEntry contains values for bib fields which
	are 'x' in the existing DataFrame. If newEntry does contain new 
	data, return an updated bibliography entry. If uid does not
	exist, return the new bibliography entry unmodified.

	Parameters
	----------
	bib : pd.DataFrame
		pandas DataFrame containing bibliography data

	newEntry : pd.Series or pd.DataFrame
		either a pandas Series object or a DataFrame containing a
		single row that can be squeezed into a Series. Contains data
		for a bibliography entry that may or may not already exist in
		the bibliography DataFrame.

	uid : string (probably)
		Label of the column containing unique identifiers for each
		bibliography entry.

	Returns
	-------
	updateResult
		A bibliograph.util.updateResult object containing a new
		bibliography entry or an updated entry. 
	'''
	if type(newEntry) is not pd.Series:
		if len(newEntry) == 1:
			newEntry = newEntry.squeeze()
		else:
			raise ValueError('newEntry must be pd.Series or pd.DataFrame-like object with a squeeze method and exactly one row.')

	if (bib[uid] == newEntry[uid]).any():
		toUpdate = bib.loc[bib[uid] == newEntry[uid]].copy()
		if len(toUpdate) == 1:
			toUpdate = toUpdate.squeeze()
			for c in bib.columns:
				if (toUpdate[c] == 'x') and (newEntry[c] != 'x'):
					toUpdate[c] = newEntry[c]
			return(updateResult(True, toUpdate, toUpdate.name))
		else:
			raise RuntimeError('Found repeated values in bibliography column ' + str(uid) + ' when processing\n' + str(newEntry))
	else:
		return(updateResult(False, newEntry))
