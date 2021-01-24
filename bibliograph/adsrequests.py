from bibliograph.util import df_progress_bar
from datetime import datetime
from os import environ
from pandas import DataFrame
from pandas import Series
from requests import get as http_get
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
    query_text = '+'.join([(str(k) + ':' + str(v)) for k,v in query_field_data.items()])
    if wrapper != '':
        query_text = wrapper + '(' + query_text + ')'

    fetch_terms = '+'.join(ads_transformers.keys())
    query_text = query_text + '&fl=' + fetch_terms

    return prefix + query_text


def submit_ads_query(ads_query, key=None):
    if key is None:
        try:
            key = os.environ['ADS_DEV_KEY']
        except KeyError:
            error_text = 'Cannot find ADS developer key to access API.\n'
            error_text += 'Directions here: https://github.com/adsabs/adsabs-dev-api#access\n'
            error_text += 'Either pass the key as a variable to submit_ads_query or create an environment variable called "ADS_DEV_KEY"'
            raise KeyError(error_text)

    r = http_get(ads_query, headers={'Authorization':'Bearer ' + key})

    return r


def parse_ads_response(ads_response, ads_transformers):
    if 'bibcode' not in ads_transformers.keys():
        ads_transformers['bibcode'] = 'copy'

    response_json = ads_response.json()
    fetched_docs = response_json['response']['docs']
    num_found = response_json['response']['numFound']

    if num_found == 0:
        return Series({}, dtype='object')
    elif num_found > 1:
        bibcodes = ' '.join([doc['bibcode'] for doc in fetched_docs])
        new_row = {'bibcode':bibcodes}
        return Series(new_row)
    elif num_found == 1:
        copy_keys = [k for k,v in ads_transformers.items() if (v == 'copy')]
        transform_keys = [k for k in ads_transformers.keys() if k not in copy_keys]
        fetched_data = fetched_docs[0]
        new_row = [ads_transformers[k](fetched_data[k]) for k in transform_keys]
        [new_row[0].update(d) for d in new_row[1:]]
        new_row = new_row[0]
        for k in copy_keys:
            new_row[k] = fetched_data[k]
        return Series(new_row)
