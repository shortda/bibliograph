import csv
import progressbar
import networkx as nx
import pandas as pd
from .util import bibUpdate
from .util import getBibtexTags
from .util import refToBib

def slurpBibTex(cn, bibTexFilename, bibcols=None, refcols='title', tag_processors=None):
	'''
	Read a BibTex file and create a pandas DataFrame for the
	bibliography.
	
	Parameters
	----------
	bibTexFilename : string
		Name of a file containing BibTex data

	bibcols : list-like
		Labels of columns the bibliography will contain. If None, the
		bibliography will contain columns for every tag in the BibTex
		file.

	refcols : list-like OR string
		Labels of columns whose values should be joined by spaces to
		create a unique reference string for each row. If string, must
		contain a column label. Defaults to 'title'.

	tag_processors : dictionary
		tag_processors is a dictionary with format 
		
			{bibTexTag:[columnName, function_to_process_bibTex]}
		or
			{bibTexTag:[[columnName1, function1_to_process_bibTex],
						[columnName2, function2_to_process_bibTex]]}

		where bibTexTag is a field in the BibTex entries, columnName
		is the label for a bibliography column where data from that
		tag should be stored, and the functions translate the text of
		the BibTex entry into a value that should be stored in the 
		bibliography column

	Returns
	-------
	bib : pd.DataFrame
		The bibliography
	'''
	texTags = getBibtexTags(bibTexFilename)

	if bibcols is None:
		bibcols = texTags

	if (type(refcols) == str) and (refcols not in bibcols):
		raise ValueError('If using an existing column instead of a "ref" column, refcols must be in bibcols.')

	if not all([c in texTags for c in bibcols]):

		if tag_processors is None:
			if not any([c in texTags for c in bibcols]):
				raise ValueError('bibcols contains no values which are tags in the bibTex file, but no translation dictionary was given.')
			else: 
				tags_to_process = []
				print('No bibTex tag translators given. bibliography columns not listed as tags in the bibTex file:\n\t', [c for c in bibcols if c not in texTags], '\n')
		else:
			if not all([t in texTags for t in tag_processors.keys()]):
				raise ValueError('tag_processors contains keys which are not tags in the bibTex file.')
			tags_to_process = tag_processors.keys()
			translated = []

		for tag in tags_to_process:
			processor = tag_processors[tag]
			if type(processor[0]) is not str:
				for thisProcessor in processor:
					print('bibTex tag translator found:', tag, '->', thisProcessor[0])
					translated.append(thisProcessor[0])
			else:
				print('bibTex tag translator found:', tag, '->', tag_processors[tag][0])
				translated.append(tag_processors[tag][0])
		print('bibliography columns not translated from bibTex data:', [c for c in bibcols if c not in translated], '\n')

	#bib = pd.DataFrame(columns=bibcols, dtype='str')
			
	for texEntry in open(bibTexFilename, encoding='utf8').read().split('@')[1:]:
		
		bibEntry = {}
		texEntry = texEntry.translate(str.maketrans('','','{}\t')).split('\n')

		for item in texEntry:
			if '=' in item:

				if item.count('=') > 1:
					item = item.split('=')
					tag, item = item[0].strip(), '='.join(item[1:])
				else:
					tag, item = item.split('=')
					tag = tag.strip()
					item = item.strip()

				if item[-1] == ',':
					item = item[:-1]

				if tag in tags_to_process:
					thisTag = tag_processors[tag]
					if type(thisTag[0]) is not str:
						for processor in thisTag:
							bibEntry[processor[0]] = processor[1](item)
					else:
						bibEntry[thisTag[0]] = thisTag[1](item)
				elif tag in bibcols:
					bibEntry[tag] = item

		if type(refcols) != str:
			bibEntry['ref'] = ''
			for key in refcols:
				if key in bibEntry.keys():
					bibEntry['ref'] += bibEntry[key] + ' '
			bibEntry['ref'] = bibEntry['ref'][:-1]

		cn.update(pd.Series(bibEntry, index=bibcols))

	#return(bib)

def slurpReferenceCSV(cn, csvname, direction='outgoing', uid='ref', noNewSources=False, separator=' | ', translator=None):
	'''
	Read a CSV file that contains reference data. File should have two
	columns and every row should have data in at most one column. If a
	row has a value in the first column, that value must be the unique
	identifier of a paper in the bibliography. If a row has a value 
	in the second column, that value is treated as bibliographic data
	to parse into a bibliography entry for a reference to or from the
	bibliography paper listed above the current row. The file looks
	like this:

		bib paper 1,
		,paper that references or is referenced by bib paper 1
		,paper that references or is referenced by bib paper 1
		bib paper 2,
		,paper that references or is referenced by bib paper 2

	Values in column two are single strings containing data separated
	by the separator parameter (default ' | ').

	Parameters
	----------
	csvname : string
		Name of csv file

	cn : bibliograph.citnet
		A citation network containing bibliographic and citation data
		to be modified

	direction : string
		Must be "outgoing" or "incoming", defaule "outgoing". If 
		citations in the csv file are outgoing, then the references in
		the file are the ones listed in the text of the papers in the
		bib DataFrame (they're references FROM papers in the
		bibliography). If citations are incoming, the references in
		file are papers whose texts contain references to papers in 
		the bib DataFrame (they're references TO papers in the
		bibliogrpahy).

	uid : string (probably)
		Label of the column containing unique identifiers for each
		bibliography entry.

	noNewSources : boolean
		If True, only get reference data for papers already listed in
		the bib DataFrame. If False, add sources in the csv file to
		the bibliography if not already listed. Default False.
	
	separator : string
		separator between bibliography fields listed for each citation
		in the csv file.

	translator : function
		function that takes list of separated fields from a reference
		listed in the csv file and returns a list-like object
		containing values for all columns in the bibliography. If the
		translator returns the single integer 0 then the translator
		found a bad entry in the csv file. Data about that line is
		stored for reporting after references are slurped. If
		translator is None, script assumes reference strings contain
		data for all bibliography columns listed in the order of
		columns in the bibliography.
	'''
	print('\tSlurping file ' + csvname)
	bib = cn.bib.copy()
	cit = cn.cit.copy()

	if direction not in ['incoming', 'outgoing']:
		raise ValueError('slurpReferenceCSV needs direction "incoming" or "outgoing" to define sources and targets in cit DataFrame.\n\tGot ' + str(direction))

	bibcols = bib.columns
	oldSources = bib[uid].copy()

	with open(csvname, 'r', encoding='utf-8') as f:
		reader = csv.reader(f, delimiter=',')
		badEntries = []
		for row in reader:
			print('\tReading row ' + str(reader.line_num), end='\r')
			if direction == 'outgoing':
				src = row[0].strip()
				tgt = row[1].strip()
			elif direction == 'incoming':
				src = row[1].strip()
				tgt = row[0].strip()

			if (src != '') and (tgt != ''):
				raise ValueError('Found the following row with two entries in ' + csvname + ':\n\t' + str(row))
			elif (src == '') and (tgt == ''):
				raise ValueError('Found row with no data at line ' + str(reader.line_num) + ' in ' + csvname)
			elif src == '':
				thisTgt = tgt.split(' | ')
				if translator is not None:
					thisTgt = translator(thisTgt)
					if thisTgt == 0:
						badEntries.append(str(reader.line_num) + '  ' + tgt)
						continue
				thisTgt = pd.Series(dict(zip(bibcols, thisTgt)), index=bibcols)
				cn.update(thisTgt, updateCit=True, src=thisSrcI)
			elif (tgt == ''): 
				if not (oldSources == src).any():
					if noNewSources:
						raise ValueError('Found source in ' + csvname + ' which is not in the bib DataFrame: ' + src)
					cn.update(refToBib(src, bibcols, cn.refcols))
				thisSrc = bib[bib[uid] == src]
				if len(thisSrc) > 1:
					raise RuntimeError('Found repeated values in bib["' + uid + '"] when processing\n' + str(thisSrc))
				thisSrcI = thisSrc.index[0]
			else:
				raise ValueError('Bad row at', reader.line_num, 'in', csvname)
		print('\tReading row ' + str(reader.line_num))
		if len(badEntries) != 0:
			raise ValueError('Found ' + str(len(badEntries)) + ' bad entries in csv file:\n\t' + '\n\t'.join(badEntries))	