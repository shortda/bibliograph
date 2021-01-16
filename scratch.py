import bibliograph as bg
import pandas as pd
import csv

bib_cols = ['fau', 'yr', 'pub', 'vl', 'bp', 'doi', 'au', 'deg', 'bibcode', 'ref']
ref_cols = bib_cols[:5]

tex_tags = ['author', 'year', 'journal', 'booktitle', 'pages', 'volume', 'doi']

with open('../dissertation/bibstems.csv', 'r', encoding='utf-8') as f:
  reader = csv.reader(f)
  abbr = {row[0]:row[1] for row in reader}
  del reader

def surnameInitialSpace(bibTexAuthors):
   au = ''
   for name in bibTexAuthors.split(' and '):
      name = name.split(', ')
      if ' ' in name[0]:
         name[0] = ''.join(name[0].split(' '))
      name = (name[0] + ''.join(c for c in name[1] if c.isupper())).lower()
      au += name + ' '
   return({'fau':au.split(' ')[0], 'au':au[:-1]})

def bpWithAlphaDash(pages):
    bp = pages.split('--')
    if bp[0].isalpha() and (len(bp) > 1):
        bp = bp[0] + '-' + bp[1]
    else:
        bp = bp[0]
    return({'bp':bp})

def getPub(pub):
    if pub.lower() in abbr.keys():
        return({'pub':abbr[pub.lower()]})
    else:
        return({'pub':pub})

def lowerDOI(doi):
   return({'doi':doi.lower()})

tex_transformers = {'author':surnameInitialSpace,
                    'year':(lambda x: {'yr':x}),
                    'journal':getPub,
                    'booktitle':getPub,
                    'pages':bpWithAlphaDash,
                    'volume':(lambda x: {'vl':x}),
                    'doi':lowerDOI}

#tex_documents = bg.readwrite.read_tex_data('../dissertation/NCARpapers.bib', tex_tags)
tex_documents = bg.readwrite.read_tex_data('../dissertation/NCARpapers1962.bib', tex_tags)
bib_documents = bg.readwrite.tex_to_bib(tex_tags, tex_documents, tex_transformers)
bib_documents = [{**doc, **{'ref':bg.util.make_ref_str(doc, ref_cols)}} for doc in bib_documents]
new_bib_rows = pd.DataFrame(columns = bib_cols, data=bib_documents)

cn = bg.CitNet() # this is from an old version of CitNet - initialization shouldn't create a df with no columns
cn.bib = new_bib_rows.fillna('x')
cn.cit = pd.DataFrame(columns=['src', 'tgt'])

def manual_parser(csv_value, manual_cols, separator='|'):
    bib_data = csv_value.split(separator)
    bib_data = [datum.strip() for datum in bib_data]
    bib_data = dict(zip(manual_cols, bib_data))
    bib_data['fau'] = bib_data['au'].split()[0]
    return pd.Series(bib_data)

#manual_data = bg.readwrite.read_manual_data('../dissertation/NCAR-referencesManual.csv', manual_parser=manual_parser)
manual_data = bg.readwrite.read_manual_data('../dissertation/NCAR1962-manual.csv', manual_parser=manual_parser)
manual_data = bg.util.add_ref_to_dataframe(manual_data, ref_cols)
