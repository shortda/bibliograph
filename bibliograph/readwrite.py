import csv
import pandas as pd
from .util import getBibtexTags
from .util import refToBib

def slurp_bibtex(cn, bibtex, refcols, bibcols=None, bibtex_parsers=None):
	'''
	Read a BibTex file and create a pandas DataFrame for the
	bibliography.
	
	Parameters
	----------
	bibtex : string
		Name of a file containing BibTex data

	bibcols : list-like
		Labels of columns the bibliography will contain. If None, the
		bibliography will contain columns for every tag in the BibTex
		file.

	refcols : list-like OR string
		Labels of columns whose values should be joined by spaces to
		create a unique reference string for each row. If string, must
		contain a column label. Defaults to 'title'.

	bibtex_parsers : dictionary
		bibtex_parsers is a dictionary with format 
		
			{bibTexTag:[columnName, function_to_process_bibTex]}
		or
			{bibTexTag:[[columnName1, function1_to_process_bibTex],
						[columnName2, function2_to_process_bibTex]]}

		where bibTexTag is a field in the BibTex entries, columnName
		is the label for a bibliography column where data from that
		tag should be stored, and the functions translate the text of
		the BibTex entry into a value that should be stored in the 
		bibliography column
	'''
	textags = getBibtexTags(bibtex)

	if bibcols is None:
		bibcols = textags

	if type(refcols) == str:
		if refcols not in bibcols:
			raise ValueError('If using an existing column instead of a "ref" column, refcols must be in bibcols.')
		else:
			refcols = [refcols]

	if not all([c in textags for c in bibcols]):

		if bibtex_parsers is None:
			if not any([c in textags for c in bibcols]):
				raise ValueError('bibcols contains no values which are tags in the bibTex file, but no dictionaryof parsers was given.')
			else: 
				tags_to_process = []
				print('No bibtex tag parsers given. bibliography columns not listed as tags in the bibTex file:')
				print('\t', [c for c in bibcols if c not in textags], '\n')
		else:
			if not all([t in textags for t in bibtex_parsers.keys()]):
				raise ValueError('bibtex_parsers contains keys which are not tags in the bibTex file.')
			tags_to_process = bibtex_parsers.keys()
			translated = []

		for tag in tags_to_process:
			processor = bibtex_parsers[tag]
			if type(processor[0]) is not str:
				for this_processor in processor:
					print('bibTex tag translator found:', tag, '->', this_processor[0])
					translated.append(this_processor[0])
			else:
				print('bibTex tag translator found:', tag, '->', bibtex_parsers[tag][0])
				translated.append(bibtex_parsers[tag][0])
		print('bibliography columns not translated from bibTex data:', [c for c in bibcols if c not in translated], '\n')
			
	for texentry in open(bibtex, encoding='utf8').read().split('@')[1:]:
		
		bibentry = {}
		texentry = texentry.translate(str.maketrans('','','{}\t')).split('\n')

		for item in texentry:
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
					this_tag = bibtex_parsers[tag]
					if type(this_tag[0]) is not str:
						for processor in this_tag:
							bibentry[processor[0]] = processor[1](item)
					else:
						bibentry[this_tag[0]] = this_tag[1](item)
				elif tag in bibcols:
					bibentry[tag] = item

		bibentry['ref'] = ''
		for key in refcols:
			if key in bibentry.keys():
				bibentry['ref'] += bibentry[key] + ' '
		bibentry['ref'] = bibentry['ref'][:-1]

		cn.update(pd.Series(bibentry, index=bibcols))

def slurp_csv(cn, csvname, direction='outgoing', sources_from_csv=False, csv_separator=' | ', csv_parser=None):
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
	by the csv_separator parameter (default ' | ').

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

	noNewSources : boolean
		If True, only get reference data for papers already listed in
		the bib DataFrame. If False, add sources in the csv file to
		the bibliography if not already listed. Default False.
	
	csv_separator : string
		separator between bibliography fields listed for each citation
		in the csv file.

	csv_parser : function
		function that takes list of separated fields from a reference
		listed in the csv file and returns a list-like object
		containing values for all columns in the bibliography. If the
		parser returns the single integer 0 then the parser
		found a bad entry in the csv file. Data about that line is
		stored for reporting after references are slurped. If
		parser is None, script assumes reference strings contain
		data for all bibliography columns listed in the order of
		columns in the bibliography.
	'''
	print('\tSlurping file ' + csvname)

	if direction not in ['incoming', 'outgoing']:
		raise ValueError('slurpReferenceCSV needs direction "incoming" or "outgoing" to define sources and targets in cit DataFrame.\n\tGot ' + str(direction))

	bibcols = cn.bib.columns
	old_sources = cn.bib['ref'].copy()

	with open(csvname, 'r', encoding='utf-8') as f:
		reader = csv.reader(f, delimiter=',')
		bad_entries = []
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
			elif (tgt == ''): 
				if not (old_sources == src).any():
					if not sources_from_csv:
						raise ValueError('Found source in ' + csvname + ' which is not in the bib DataFrame: ' + src)
					cn.update(refToBib(src, bibcols, cn.refcols))
					this_src_idx = cn.bib.index[-1]
				else:
					this_src = cn.bib[cn.bib['ref'] == src]
					if len(this_src) > 1:
						raise RuntimeError('Found repeated values in cn.bib["ref"] when processing\n' + str(this_src))
					this_src_idx = this_src.index[0]
			elif src == '':
				this_tgt = tgt.split(csv_separator)
				if csv_parser is not None:
					this_tgt = csv_parser(this_tgt)
					if this_tgt == 0:
						bad_entries.append(str(reader.line_num) + '  ' + tgt)
						continue
				this_tgt = pd.Series(dict(zip(bibcols, this_tgt)), index=bibcols)
				cn.update(this_tgt, updateCit=True, src=this_src_idx)
			else:
				raise ValueError('Bad row at', reader.line_num, 'in', csvname)
		print('\tReading row ' + str(reader.line_num))
		if len(bad_entries) != 0:
			raise ValueError('Found ' + str(len(bad_entries)) + ' bad entries in csv file:\n\t' + '\n\t'.join(bad_entries))	