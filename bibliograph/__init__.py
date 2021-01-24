
from bibliograph.citnet import CitNet
from bibliograph.readwrite import *
from bibliograph.adsrequests import *
from bibliograph.util import *
from bibliograph.scrub import *

'''
# NEW PROCESS TO GET BIBTEX, ADS DATA, 2021-01-13
import bibliograph as bg
import pandas as pd
import csv
from datetime import datetime

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

def getPub(publication):
  return({'pub':abbr[publication.lower()]})


def lowerDOI(doi):
   return({'doi':doi.lower()})

tex_transformers = {'author':surnameInitialSpace,
                    'year':(lambda x: {'yr':x}),
                    'journal':getPub,
                    'booktitle':getPub,
                    'pages':bpWithAlphaDash,
                    'volume':(lambda x: {'vl':x}),
                    'doi':lowerDOI}

tex_documents = bg.readwrite.read_tex_data('../dissertation/NCARpapers1962.bib', tex_tags)
bib_documents = bg.readwrite.tex_to_bib(tex_tags, tex_documents, tex_transformers)
bib_documents = [{**doc, **{'ref':bg.util.make_ref_str(doc, ref_cols)}} for doc in bib_documents]
new_bib_rows = pd.DataFrame(columns = bib_cols, data=bib_documents)

cn = bg.CitNet() # this is from an old version of CitNet - initialization shouldn't create a df with no columns
cn.bib = new_bib_rows

# get ads data for these documents

bib_transformers = {'yr':(lambda x: {'year':x}),
                    'pub':(lambda x: {'bibstem':x}),
                    'vl':(lambda x: {'volume':x}),
                    'bp':(lambda x: {'page':x})}

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

query_maker = lambda x: bg.adsrequests.make_ads_query(x, bib_transformers, ads_transformers)
queries = cn.apply(query_maker, axis=1)
tot = len(queries)
responses = []
for q in queries:
  responses.append(bg.adsrequests.submit_ads_query(q))
  bg.util.df_progress_bar(tot, len(responses), prefix='ADS queries completed')
print()
print('API rate limit:', responses[-1].headers['X-RateLimit-Limit'])
print('queries remaining:', responses[-1].headers['X-RateLimit-Remaining'])
reset = datetime.utcfromtimestamp(int(responses[-1].headers['X-RateLimit-Reset'])).strftime('%H:%M:%S, %Y-%m-%d')
print('rate limit resets at ' + str(reset))
responses = pd.Series(responses)
parser = lambda x: bg.adsrequests.parse_ads_response(x, ads_transformers)
parsed_responses = responses.apply(parser)
parsed_responses = parsed_responses.fillna('?')
parsed_with_ref = bg.util.add_ref_to_dataframe(parsed_responses, ref_cols)
parsed_with_ref.apply((lambda x: cn.update_entry(x, fillna='?')), axis=1)
'''