import csv
import pandas as pd
from .util import make_ref_str
from .util import rawcount
from .util import df_progress_bar
from .util import expand_ref_str
from .util import ManualQuoteError
from .util import ManualLineError

def read_bibtex_tags(bibtex, skiptags=[]):
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

def read_tex_data(fname, tex_tags):
    tags_in_file = read_bibtex_tags(fname)

    check = [t in tags_in_file for t in tex_tags]
    if not all(check):
        #raise ValueError('not all tex_tags were found in file ' + fname)
        print('The following tex tags were provided in tex_tags but not found in ' + fname)
        for t in [t for t in tex_tags if t not in tags_in_file]:
            print('\t', t)

    with open(fname, encoding='utf8') as f:

        tex_documents = []

        for texentry in f.read().split('@')[1:]:

            texentry = texentry.translate(str.maketrans('','','{}\t'))
            texentry = texentry.split('\n')

            this_document = {}

            for item in texentry:
                if '=' in item:
                    if item.count('=') > 1:
                        item = item.split('=')
                        tag, item = item[0].strip(), '='.join(item[1:])
                    else:
                        tag, item = item.split('=')
                        tag = tag.strip()
                        item = item.strip()

                    if tag in tex_tags:
                        if item[-1] == ',':
                            item = item[:-1]

                        this_document[tag] = item

            if len(this_document) > 0:
                tex_documents.append(this_document)

    return tex_documents

def tex_to_bib(tex_tags, tex_documents, tex_transformers):

    doc_keys = set().union(*(d.keys() for d in tex_documents))
    transform_tags = [t for t in tex_transformers.keys() if t in doc_keys]
    copy_tags = [t for t in tex_tags if ((t not in transform_tags) and (t in doc_keys))]

    bib_documents = []
    for doc in tex_documents:
        bib_doc = [tex_transformers[t](doc[t]) for t in transform_tags if t in doc.keys()]
        [bib_doc.append({t:doc[t]}) for t in copy_tags if t in doc.keys()]
        #for tag in copy_tags:
        #    bib_doc.append({tag:doc[tag]})
        [bib_doc[0].update(d) for d in bib_doc[1:]]
        bib_documents.append(bib_doc[0])

    return bib_documents


def read_manual_data(fname, manual_parser=None, direction='outgoing', separator='|', special='x'):

    if direction not in ['incoming', 'outgoing']:
        raise ValueError('direction must be "incoming" or "outgoing". Got ' + str(direction))

    file_length = rawcount(fname)
    file_length_mod_10 = file_length % 10

    with open(fname, 'r', encoding='utf-8') as f:

        first = f.readline()
        if 'ref_cols:' in first:
            ref_cols = first.split('ref_cols:')[1]
            ref_cols = [c.strip() for c in ref_cols.split(',')]
            print('found csv data with reference columns', ref_cols)
        else:
            raise ValueError('manual csv files must have a top line beginning "ref_cols:" that contains comma-separated bibliography column labels that make up the ref string')

        second = f.readline()
        if 'manual_cols:' in second:
            manual_cols = second.split('manual_cols:')[1]
            manual_cols = [c.strip() for c in manual_cols.split(',')]
            print('found csv data with manual columns', manual_cols)
        else:
            raise ValueError('manual csv files must have a second line beginning "manual_cols:" that contains comma-separated labels of bibliography columns to be populated by manually entered lines')

    if manual_parser is None:
        def manual_parser(csv_value, manual_cols, separator=separator):
            bib_data = csv_value.split(separator)
            bib_data = [datum.strip() for datum in bib_data]
            return pd.Series(dict(zip(manual_cols, bib_data)))

    def conditional_parser(line):
        line = line.strip()
        if line[0] == ',':
            line = line[1:]
            if '"' in line:
                if line.count('"') != 2:
                    raise ManualQuoteError(num_quotes=line.count('"'))
                return (1, manual_parser(line.split('"')[1], manual_cols, separator), '')
            else:
                return (1, manual_parser(line.strip(',').strip(), manual_cols, separator), '')
        elif line[-1] == ',':
            ref = line[:-1].strip()
            return (0, expand_ref_str(ref, ref_cols), ref)
        else:
            raise ManualLineError()

    with open(fname, 'r', encoding='utf-8') as f:
        f.readline()
        f.readline()
        manual_data = []
        line_num = 3
        last_bib_ref = ''

        if direction == 'outgoing':
            cit_col = 'src'
        elif direction == 'incoming':
            cit_col = 'tgt'

        for line in f:
            try:
                csv_col, doc_data, bib_ref = conditional_parser(line)
            except ManualQuoteError as err:
                err.set_line_num(line_num)
                err.set_fname(fname)
                raise
            except ManualLineError as err:
                err.set_line_num(line_num)
                err.set_fname(fname)
                raise

            if csv_col == 1:
                doc_data[cit_col] = last_bib_ref
                manual_data.append(doc_data)
            elif csv_col == 0:
                doc_data[cit_col] = special
                last_bib_ref = bib_ref
                manual_data.append(doc_data)

            if ((line_num + file_length_mod_10) % 10 == 0):
                df_progress_bar(file_length, line_num, prefix='Reading CSV file')
            line_num += 1

    print()
    return pd.DataFrame(manual_data).fillna(special)

def slurp_bibtex(cn, fname, refcols, bibcols=None, bibtex_parsers=None):
    '''
    Read a BibTex file and create a pandas DataFrame for the
    bibliography.

    Parameters
    ----------
    fname : string
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
    textags = get_bibtex_tags(fname)

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
                print('No bibtex tag parsers given. bibliography columns not listed as tags in the BibTex file:')
                print('\t', [c for c in bibcols if c not in textags], '\n')
        else:
            if not all([t in textags for t in bibtex_parsers.keys()]):
                print('The following bibtex_parsers keys were not found in the BibTex file:')
                print([t for t in bibtex_parsers.keys() if t not in textags])
                print()
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

    for texentry in open(fname, encoding='utf8').read().split('@')[1:]:

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

        cn.update_entry(pd.Series(bibentry, index=bibcols))

    print('Slurped bibtex file ' + fname + '\nDescription:')
    print(cn.describe(), '\n')

def slurp_csv(cn, fname, direction='outgoing', sources_from_csv=True, csv_separator=' | ', csv_parser=None):
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
    fname : string
    Name of csv file

    cn : bibliograph.CitNet
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

    sources_from_csv : boolean
    If True, add sources in the csv file tothe bibliography if not
    already listed. If False, raise error when a sources is found
    in the csv file but not in the bibliography. Default True.

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
    print('\tSlurping file ' + fname)

    if direction not in ['incoming', 'outgoing']:

        raise ValueError('slurp_csv needs direction "incoming" or "outgoing" to define sources and targets in cit DataFrame.\n\tGot ' + str(direction))

    bibcols = cn.bib.columns
    old_sources = cn.bib['ref'].copy()

    with open(fname, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=',')
        bad_entries = []

        if len(bibcols) == 0:
            bibcols = next(f).split(',')
            if len(bibcols) == 1:
                raise ValueError('slurp_csv got an empty bibliography but the first line of ' + fname + ' contains only one value.')
            refcols = next(f).split(',')
            if len(refcols) == 1:
                raise ValueError('slurp_csv got an empty bibliography but the second line of ' + fname + ' contains only one value.')
            print('slurp_csv got an empty bibliography. created the following from top two lines in ' + fname)
            cn.bib = pd.DataFrame(columns=bibcols)
            cn.refcols = refcols
            print(cn.describe())
            print('refcols = ', refcols)
        else:
            print('slurp_csv got a bibliography with columns')
            print('\t', bibcols)

        for row in reader:
            print('\tReading row ' + str(reader.line_num), end='\r')
            if direction == 'outgoing':
                src = row[0].strip()
                tgt = row[1].strip()
            elif direction == 'incoming':
                src = row[1].strip()
                tgt = row[0].strip()

            if (src != '') and (tgt != ''):
                raise ValueError('Found the following row with two entries in ' + fname + ':\n\t' + str(row))
            elif (src == '') and (tgt == ''):
                raise ValueError('Found row with no data at line ' + str(reader.line_num) + ' in ' + fname)
            elif (tgt == ''):
                if not (old_sources == src).any():
                    if not sources_from_csv:
                        raise ValueError('Found source in ' + fname + ' which is not in the bib DataFrame: ' + src)
                    cn.update_entry(refToBib(src, bibcols, cn.refcols))
                    this_src_idx = cn.bib.index[-1]
                else:
                    this_src = cn.bib[cn.bib['ref'] == src]
                    if len(this_src) > 1:
                        raise RuntimeError('Found repeated values in CitNet.bib["ref"] when processing\n' + str(this_src))
                    this_src_idx = this_src.index[0]
            elif src == '':
                this_tgt = tgt.split(csv_separator)
                if csv_parser is not None:
                    this_tgt = csv_parser(this_tgt)
                    if this_tgt == 0:
                        bad_entries.append(str(reader.line_num) + '  ' + tgt)
                        continue
                this_tgt = pd.Series(dict(zip(bibcols, this_tgt)), index=bibcols)
                cn.update_entry(this_tgt, update_citations=True, src=this_src_idx)
            else:
                raise ValueError('Bad row at', reader.line_num, 'in', fname)

        print('\tReading row ' + str(reader.line_num))

        if len(bad_entries) != 0:

            raise ValueError('Found ' + str(len(bad_entries)) + ' bad entries in csv file:\n\t' + '\n\t'.join(bad_entries))

    print('Slurped csv file ' + fname + '\nDescription:')
    print(cn.describe(), '\n')
