'''
import bibliograph as bg
import pandas as pd
import csv

bib_cols = ['fau', 'yr', 'pub', 'vl', 'bp', 'doi', 'au', 'deg', 'bibcode', 'ref']
ref_cols = bib_cols[:5]

cn = bg.CitNet(bib_cols)

tex_tags = ['author', 'year', 'journal', 'booktitle', 'pages', 'volume', 'doi']

with open('../dissertation/bibstems.csv', 'r', encoding='utf-8') as f:
  reader = csv.reader(f)
  abbr = {''.join(row[0].split()):row[1] for row in reader}
  del reader

def surnameInitialSpace(bibTexAuthors):
    au = ''
    for name in bibTexAuthors.split(' and '):
        name = name.split(', ')
        if ' ' in name[0]:
            name[0] = ''.join(name[0].split(' '))
        if len(name) > 1:
            name = (name[0] + ''.join(c for c in name[1] if c.isupper())).lower()
        else:
            name = name[0]
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
    if ''.join(pub.lower().split()) in abbr.keys():
        return({'pub':abbr[''.join(pub.lower().split())]})
    else:
        return({'pub':''.join(pub.split())})

def lowerDOI(doi):
   return({'doi':doi.lower().strip('https://doi.org/')})

tex_transformers = {'author':surnameInitialSpace,
                    'year':(lambda x: {'yr':x}),
                    'journal':getPub,
                    'booktitle':getPub,
                    'pages':bpWithAlphaDash,
                    'volume':(lambda x: {'vl':x}),
                    'doi':lowerDOI}

tex_documents = bg.read_tex_data('../dissertation/NCARpapers1992kludge.bib', tex_tags)
#tex_documents = bg.read_tex_data('../dissertation/NCARpapers1962.bib', tex_tags)
bib_documents = bg.tex_to_bib(tex_tags, tex_documents, tex_transformers)
bib_documents = [{**doc, **{'ref':bg.make_ref_str(doc, ref_cols)}} for doc in bib_documents]
new_bib_rows = pd.DataFrame(columns = bib_cols, data=bib_documents).fillna('x')

print('importing tex documents')
cn.import_documents(new_bib_rows)
cn.bib.deg = 0

def manual_parser(csv_value, manual_cols, separator='|'):
    bib_data = csv_value.split('__')
    bib_data = [' '.join(bib_data[0].split('_')), *bib_data[1].split('_')]
    bib_data = [datum.strip() for datum in bib_data]
    bib_data = dict(zip(manual_cols, bib_data))
    bib_data['pub'] = getPub(bib_data['pub'])['pub']
    bib_data['fau'] = bib_data['au'].split()[0]
    if 'doi' in bib_data.keys():
        bib_data['doi'] = bib_data['doi'].strip('https://doi.org/')
    return pd.Series(bib_data)

manual_data = bg.read_manual_data('../dissertation/NCAR-referencesManual-working.csv', manual_parser=manual_parser)
#manual_data = bg.read_manual_data('../dissertation/NCAR1962-manual.csv', manual_parser=manual_parser)
manual_data = bg.add_ref_to_dataframe(manual_data, ref_cols)

print('importing manual documents')
cn.import_documents(manual_data[[c for c in manual_data.columns if c not in ['src','tgt']]])
print('importing manual citations')
cn.import_citations(manual_data)
'''
'''
bib_transformers = {'yr':(lambda x: {'year':x}),
                    'pub':(lambda x: {'bibstem':x}),
                    'vl':(lambda x: {'volume':x}),
                    'bp':(lambda x: {'page':x})}
'''
'''
#bib_transformers = {'doi':'copy'}
bib_transformers = {'bibcode':'copy'}
def ads_surnameInitialSpace(authorList):
  authors = []
  for a in authorList:
    if ',' in a:
      a = a.split(', ')
      authors.append((''.join(a[0].split(' ')) + ''.join(c for c in a[1] if c.isupper())).lower())
    else:
      authors.append((''.join(a.split(' '))).lower())
  return {'fau':authors[0], 'au':' '.join(authors)}

ads_transformers = {'author':ads_surnameInitialSpace,
                    'year':(lambda x: {'yr':x}),
                    'bibstem':(lambda x: {'pub':x[0]}),
                    'volume':(lambda x: {'vl':x}),
                    'page':(lambda x: {'bp':x[0]}),
                    'doi':(lambda x: {'doi':x[0].lower()}),
                    'bibcode':'copy'}
'''
'''
ads_transformers = {'doi':(lambda x: {'doi':x[0].lower()}),
                    'bibcode':'copy'}
'''
'''

#print('getting dois and bibcodes for cn.bib from NASA/ADS')
#ads_data = bg.ads_from_docs(cn.bib, bib_transformers, ads_transformers)
#parsed_responses = ads_data['r'].apply(parse_ads_response, args=(ads_transformers,))
#newSources = cn.bib[(~cn.bib.ref.isin(cn.cit.src) & cn.bib.deg==0) & cn.bib.pub.apply(lambda x: len(x)<6 and (x in abbr.values()))]
#ads_data = bg.ads_from_docs(newSources.iloc[:2], bib_transformers, ads_transformers, wrapper='references')
#ads_data.to_pickle('../dissertation/ads_raw_http.pickle')

def make_manual_string(doc):
  doc = doc.squeeze()
  strings = doc[['au', 'yr', 'pub', 'vl', 'bp']].values
  strings[0] = '_'.join(strings[0].split(' '))
  return strings[0] + '__' + '_'.join(strings[1:])

def make_manual_row(bib_row, deg, bib, cit, out):
    if bib_row['deg'] != deg:
        return
    pos = out.shape[0] + 1
    out.loc[pos] = [bib_row['ref'], '']
    if bib_row['ref'] in cit['src'].values:
        links = cit.loc[cit['src']==bib_row['ref']]
        for i in links.index:
            pos = pos + 1
            tgt_value = make_manual_string(bib.loc[bib['ref']==cit.loc[i, 'tgt']])
            out.loc[pos] = ['', tgt_value]

out = pd.DataFrame(columns = ['src', 'tgt'])
cn.bib.sort_values(by=['yr','fau']).apply(make_manual_row, args=(0, cn.bib, cn.cit, out), axis=1)
out.to_excel('../dissertation/NCAR-referencesManual-auto.xlsx', index=False)
out.to_csv('../dissertation/NCAR-referencesManual-auto.csv', index=False)
'''
'''
manual_data = pd.read_csv('../dissertation/NCAR-referencesManual.csv', skiprows=2, header=None)

def manual_converter(tgt):
	tgt = tgt.split(' | ')
	tgt[0] = '_'.join(tgt[0].split())
	tgt[1] = tgt[0] + '__' + tgt[1]
	tgt[2] = ''.join(tgt[2].split())
	return '_'.join(tgt[1:])

manual_data.loc[manual_data[1].notna(), 1] = manual_data.loc[manual_data[1].notna(), 1].apply(manual_converter)

def manual_parser(csv_value, manual_cols, separator='_', auth_sep=None):
    if auth_sep is not None:
    	bib_data = csv_value.split(auth_sep)
    	bib_data = [' '.join(v[0].split(separator))] + v[1].split(separator)
    else:
    	bib_data = csv_value.split(separator)
    bib_data = [datum.strip() for datum in bib_data]
    bib_data = dict(zip(manual_cols, bib_data))
    bib_data['pub'] = getPub(bib_data['pub'])['pub']
    bib_data['fau'] = bib_data['au'].split()[0]
    if 'doi' in bib_data.keys():
    	bib_data['doi'] = bib_data['doi'].strip('https://doi.org/')
    return pd.Series(bib_data)
'''

import bibliograph as bg
import pandas as pd

bib_cols = ['fau', 'yr', 'pub', 'vl', 'bp', 'doi', 'au', 'deg', 'bibcode', 'ref']
ref_cols = bib_cols[:5]

cn = bg.CitNet(bib_cols)
cn.bib = pd.read_pickle('../dissertation/workingbib.pickle')
cn.cit = pd.read_pickle('../dissertation/workingcit.pickle')

bib_transformers = {'bibcode':'copy'}
def ads_surnameInitialSpace(authorList):
  authors = []
  for a in authorList:
    if ',' in a:
      a = a.split(', ')
      authors.append((''.join(a[0].split(' ')) + ''.join(c for c in a[1] if c.isupper())).lower())
    else:
      authors.append((''.join(a.split(' '))).lower())
  return {'fau':authors[0], 'au':' '.join(authors)}

ads_transformers = {'author':ads_surnameInitialSpace,
                    'year':(lambda x: {'yr':x}),
                    'bibstem':(lambda x: {'pub':x[0]}),
                    'volume':(lambda x: {'vl':x}),
                    'page':(lambda x: {'bp':x[0]}),
                    'doi':(lambda x: {'doi':x[0].lower()}),
                    'bibcode':'copy'}

key = 'dpD5xhbkRjEM9ydKnruCnFsb7vDUGhjHOlzInnEB'

toQuery=cn.bib.loc[(cn.bib.bibcode != 'x') & ~cn.bib.ref.isin(cn.cit.src)]
ads_data = bg.ads_from_docs(toQuery, bib_transformers, ads_transformers, wrapper='references', key=key)
ads_data.to_pickle('../dissertation/ads_raw_http.pickle')