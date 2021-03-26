from bibliograph.util import df_progress_bar
from datetime import datetime
from os import environ
from pandas import DataFrame
from pandas import isna
from pandas import Series
from requests import get as http_get
from sys import exc_info
from tqdm import tqdm
from traceback import print_tb as print_traceback
from urllib.parse import quote as urlquote


def make_ads_query(row_to_query, bib_transformers, ads_transformers, wrapper=''):

    if type(row_to_query) is not Series:
        if len(row_to_query) == 1:
            row_to_query = row_to_query.squeeze()
        else:
            raise ValueError('row_to_query must be pd.Series or pd.DataFrame-like object with a squeeze method and exactly one row.')

    if wrapper not in ['', 'references', 'citations']:
        raise ValueError('ADS query wrapper must be in ["", "references", or "citations"]')

    prefix = 'https://api.adsabs.harvard.edu/v1/search/query?q='

    bib_cols = row_to_query.index
    copy_keys = [k for k,v in bib_transformers.items() if (v == 'copy')]
    transform_keys = [k for k in bib_transformers.keys() if k not in copy_keys]

    query_field_data = [bib_transformers[key](row_to_query[key]) for key in transform_keys]
    for key in copy_keys:
        query_field_data.append({key:row_to_query[key]})
    [query_field_data[0].update(d) for d in query_field_data[1:]]
    query_field_data = query_field_data[0]
    if 'bibstem' in query_field_data.keys():
        if query_field_data['bibstem'] == 'JGR':
            if 'page' in query_field_data.keys():
                p = query_field_data['page']
                if (len(p) > 4) and (len(p) < 7):
                    query_field_data['page'] = p[:-3] + ',' + p[-3:]
        if 'JGR' in query_field_data['bibstem']:
            if 'year' in query_field_data.keys():
             if (query_field_data['year'].isdigit()) and (int(query_field_data['year']) < 1978):
                 query_field_data['bibstem'] = 'JGR'
        if query_field_data['bibstem'] == 'RvGeo':
            if 'year' in query_field_data.keys():
                if query_field_data['year'].isdigit() and (int(query_field_data['year']) < 1985):
                    query_field_data['bibstem'] = 'RvGSP'
    for k,v in query_field_data.items():
        v = str(v)
        if any([c in v for c in '():']):
            query_field_data[k] = '"' + v + '"'
        query_field_data[k] = urlquote(v)
    query_text = '+'.join([(str(k) + ':' + v) for k,v in query_field_data.items()])
    if wrapper != '':
        query_text = wrapper + '(' + query_text + ')'

    fetch_terms = '+'.join(ads_transformers.keys())
    query_text = query_text + '&fl=' + fetch_terms + '&rows=2000'

    return prefix + query_text


def submit_ads_query(ads_query, key=None):
    if key is None:
        try:
            key = environ['ADS_DEV_KEY']
        except KeyError:
            error_text = 'Cannot find ADS developer key to access API.\n'
            error_text += 'Directions here: https://github.com/adsabs/adsabs-dev-api#access\n'
            error_text += 'Either pass the key as a variable to submit_ads_query or create an environment variable called "ADS_DEV_KEY"'
            raise KeyError(error_text)

    r = http_get(ads_query, headers={'Authorization':'Bearer ' + key})

    return r

def parse_ads_doc(fetched_data, ads_transformers):
    copy_keys = [k for k,v in ads_transformers.items() if (v == 'copy')]
    transform_keys = [k for k in ads_transformers.keys() if k not in copy_keys]
    output_doc = [ads_transformers[k](fetched_data[k]) for k in transform_keys if k in fetched_data.keys()]
    [output_doc[0].update(d) for d in output_doc[1:]]
    output_doc = output_doc[0]
    for k in copy_keys:
        output_doc[k] = fetched_data[k]
    return output_doc

def parse_ads_response(ads_response, ads_transformers, wrapper=''):
    if wrapper not in ['', 'references', 'citations']:
        raise ValueError('ADS query wrapper must be in ["", "references", or "citations"]')

    if isna(ads_response):
        return ads_response

    #if 'bibcode' not in ads_transformers.keys():
    #    ads_transformers['bibcode'] = 'copy'

    response_json = ads_response.json()
    fetched_docs = response_json['response']['docs']
    num_found = response_json['response']['numFound']

    if num_found == 0:
        return DataFrame(columns=ads_transformers.keys())
    elif num_found > 1:
        return DataFrame([parse_ads_doc(d, ads_transformers) for d in fetched_docs])
    elif num_found == 1:
        '''
        print(fetched_docs)
        copy_keys = [k for k,v in ads_transformers.items() if (v == 'copy')]
        transform_keys = [k for k in ads_transformers.keys() if k not in copy_keys]
        fetched_data = fetched_docs[0]
        new_row = [ads_transformers[k](fetched_data[k]) for k in transform_keys if k in fetched_data.keys()]
        [new_row[0].update(d) for d in new_row[1:]]
        new_row = new_row[0]
        for k in copy_keys:
            new_row[k] = fetched_data[k]
        '''
        new_row = parse_ads_doc(fetched_docs[0], ads_transformers)
        return DataFrame(Series(new_row)).T


def ads_from_docs(docs, bib_transformers, ads_transformers, wrapper='', key=None):
    if wrapper not in ['', 'references', 'citations']:
        raise ValueError('ADS query wrapper must be in ["", "references", or "citations"]')
        
    if type(docs) == DataFrame:
        queries = docs.apply(make_ads_query, args=(bib_transformers, ads_transformers, wrapper), axis=1)
    elif type(docs) == Series:
        queries = Series(make_ads_query(docs, bib_transformers, ads_transformers, wrapper))

    queries = DataFrame({'q':queries, 'r':Series(dtype='object')})

    if key is None:
        try:
            key = environ['ADS_DEV_KEY']
        except KeyError:
            error_text = 'Cannot find ADS developer key to access API.\n'
            error_text += 'Directions here: https://github.com/adsabs/adsabs-dev-api#access\n'
            error_text += 'Either pass the key as a variable to submit_ads_query or create an environment variable called "ADS_DEV_KEY"'
            raise KeyError(error_text)
    try:
        for i in tqdm(queries.index):
            r = submit_ads_query(queries.loc[i, 'q'], key=key)
            queries.loc[i, 'r'] = r

        if type(docs) == DataFrame:
            queries.reindex(index=docs.index)
            for col in ['doi', 'bibcode', 'ref']:
                if col in docs.columns:
                    queries[col] = docs[col]

        print()
        last = queries.loc[queries['r'].notna(), 'r'].iloc[-1]
        print('API rate limit:', last.headers['X-RateLimit-Limit'])
        print('queries remaining:', last.headers['X-RateLimit-Remaining'])
        reset = datetime.utcfromtimestamp(int(last.headers['X-RateLimit-Reset'])).strftime('%H:%M:%S, %Y-%m-%d')
        print('rate limit resets at ' + str(reset))
        
        return queries
    
    except Exception as e:
        print()
        print('\tERROR')
        print('\t', type(e), '\t', e)
        print('\t', e.__doc__)
        print_traceback(exc_info()[2])
        return queries