import csv
from bibliograph.util import rawcount
from bibliograph.util import df_progress_bar
from bibliograph.util import expand_ref_str
from bibliograph.util import ManualQuoteError
from bibliograph.util import ManualLineError
from pandas import DataFrame
from pandas import Series

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
            return Series(dict(zip(manual_cols, bib_data)))

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

            if ((line_num + file_length_mod_10) % 50 == 0):
                df_progress_bar(file_length, line_num, prefix='Reading CSV file')
            line_num += 1

    df_progress_bar(file_length, file_length, prefix='Reading CSV file')
    print()
    return DataFrame(manual_data).fillna(special)
