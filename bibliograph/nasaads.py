import ads
import pandas as pd
from .util import df_progress_bar
from datetime import datetime

def make_query(source, queries, fields, wrapper):

    query = []

    for f in fields:
        value = str(source[f[0]])
        if (' ' not in value) and (value != 'x'):
            query.append(f[1] + ':' + value)
        else:
            queries.loc[source.name] = 'x'

    query = ' '.join(query)
    if wrapper is not None:
        query = wrapper + '(' + query + ')'
    queries.loc[source.name] = query

'''
def make_queries(sources, search_cols, ads_search_terms=None, query_mask=None, wrapper=None):

    Make strings that represent ADS search queries

    Parameters
    ----------
    sources : pd.DataFrame
        Papers for which data will be found on NASA/ADS.

    search_cols : list-like
        Column labels in the sources DataFrame which contain data for
        the ADS search queries.

    ads_search_terms:
        ADS search terms corrensponding to the column labels in
        search_cols. If None, assume the columns labels are ads
        search terms. List of ADS terms is in the drop-down menu above
        the search bar at https://ui.adsabs.harvard.edu/

    query_mask : pd.DataFrame, dtype == boolean
        A boolean mask to select which sources should be queried.

    wrapper : string
        An ADS operator to wrap the query string, such as
        'references'. List of ADS operators is in the drop-down menu
        above the search bar at https://ui.adsabs.harvard.edu/

    Returns
    -------
    queries : pd.DataFrame
        pandas DataFrame with query strings whose index corresponds to
        the index of the correpsonding papers in the sources DataFrame

    bad_queries : list
        List of index values from the sources DataFrame for which
        values in columns to be searched either contained spaces or
        were 'x'.

    if query_mask is not None:
        sources = sources[query_mask]

    if ads_search_terms is not None:
        fields = [[c, ads_search_terms[i]] for i,c in enumerate(search_cols)]
    else:
        fields = [[c, c] for c in search_cols]

    print(search_cols)
    print(ads_search_terms)
    print(fields)

    queries = pd.DataFrame(columns=['qstr'], index=sources.index)
    #sources.apply(lambda x: make_query(x, queries, fields, wrapper), axis=1)
    for i in sources.index:
        query = []

        for f in fields:
            print(f)
            value = str(sources.loc[i, f[0]])
            print(value)
            if (' ' not in value) and (value != 'x'):
                query.append(f[1] + ':' + value)

        if len(query) == 0:
            queries.loc[i] = 'x'
        else:
            query = ' '.join(query)
            if wrapper is not None:
                query = wrapper + '(' + query + ')'
            queries.loc[i] = query

    return(queries)
'''

def make_queries(sources, search_cols, ads_dict=None, query_mask=None, wrapper=None):
    '''
    Make strings that represent ADS search queries

    Parameters
    ----------
    sources : pd.DataFrame
        Papers for which data will be found on NASA/ADS.

    search_cols : list-like
        Column labels in the sources DataFrame which contain data for
        the ADS search queries.


    ads_dict:


    query_mask : pd.DataFrame, dtype == boolean
        A boolean mask to select which sources should be queried.

    wrapper : string
        An ADS operator to wrap the query string, such as
        'references'. List of ADS operators is in the drop-down menu
        above the search bar at https://ui.adsabs.harvard.edu/

    Returns
    -------
    queries : pd.DataFrame
        pandas DataFrame with query strings whose index corresponds to
        the index of the correpsonding papers in the sources DataFrame

    bad_queries : list
        List of index values from the sources DataFrame for which
        values in columns to be searched either contained spaces or
        were 'x'.
    '''

    if ads_dict is not None:
        ads_search_terms = [ads_dict[c] for c in search_cols]

    if query_mask is not None:
        sources = sources[query_mask]

    queries = pd.DataFrame(columns=['qstr'], index=sources.index)
    #sources.apply(lambda x: make_query(x, queries, fields, wrapper), axis=1)

    for i in sources.index:
        query = []

        for c in search_cols:
            value = str(sources.loc[i, c])
            if (' ' not in value) and (value != 'x'):
                query.append(ads_dict[c] + ':' + value)

        if len(query) == 0:
            queries.loc[i] = 'x'
        else:
            query = ' '.join(query)
            if wrapper is not None:
                query = wrapper + '(' + query + ')'
            queries.loc[i] = query

    return(queries)

def confirm_ads_submission(queries):
    # ratelimit API calls aren't working right now; this is a dummy function for now
    return True
    '''
    Get the current rate limits on the NASA/ADS API token for this
    system, report values, and ask user to proceed if there may be
    less than 10% of the query limit available after submitting this
    set of queries. The script doesn't check if the queries DataFrame
    contains 'x' values, so the estimate of 10% remaining is
    conservative.

    NOTE: you use one API query to get the remaining number of queries

    Parameters
    ----------
    queries : pd.DataFrame
        queries to submit to the API.

    Returns
    -------
    boolean
        False if user decides not to proceed when close to rate limit.
    '''
    numqueries = len(queries.index)
    q = ads.SearchQuery(q='q').execute()
    limits = ads.RateLimits('SearchQuery').limits
    reset = datetime.utcfromtimestamp(int(limits['reset'])).strftime('%H:%M:%S, %Y-%m-%d')
    if numqueries > int(limits['limit']):
        raise ValueError('\nTrying to run up to ' + str(numqueries) + ' NASA/ADS search queries but the daily limit is ' + limits['limit'] + '\n')
    elif numqueries > int(limits['remaining']):
        raise ValueError('\nTrying to run up to ' + str(numqueries) + ' NASA/ADS search queries but this API token only has ' + limits['remaining'] + ' queries remaining today.\n\tRate limit resets at ' + reset + '\n')
    else:
        remainder = int(limits['remaining']) - numqueries
        print('\nAbout to run ' + str(numqueries) + ' NASA/ADS search queries.\nThere will be ' + str(remainder) + ' queries available today after this operation.\nRate limit resets at ' + reset + '\n')
        if remainder <= (int(limits['limit'])*0.1):
            answer = input('Remainder will likely be less than 10% of the daily limit. Enter y to continue, anything else to break.\n')
            if (answer != 'y') and (answer != 'Y'):
                return(True)
            else:
                return(False)
        return(True)


def submit_ads_queries(this_q, queries, results, fetch_cols, ads_fetch_terms, article_processor):

    print(ads_fetch_terms)
    search = ads.SearchQuery(q=this_q['qstr'], fl=ads_fetch_terms)
    try:
        print('submitted query "' + this_q['qstr'] + '"')
        search.execute()
    finally:
        results.to_json('results.json')
        queries.to_json('queries.json')
    this_q['ADSarticles'] = search.articles
    fetched = [(article_processor(art) + [this_q.name]) for art in search.articles]
    results = results.append(pd.Series(dict(zip(fetch_cols, fetched))), ignore_index=True)
    df_progress_bar(len(queries), this_q.name)

def query_bibcodes(sources, search_cols, ads_search_terms=None, query_mask=None):
    '''
    Get ADS bibcodes for papers in the sources DataFrame

    Parameters
    ----------
    sources : pd.DataFrame
        Papers for which bibcodes will be found on NASA/ADS.

    search_cols : list-like
        Column labels in the sources DataFrame which contain data for
        the ADS search queries.

    ads_search_terms:
        ADS search terms corrensponding to the column labels in
        search_cols. Length and order of terms in this list must
        correspond to length and order of columns listed in
        search_cols. If None, assume the column labels are ADS
        search terms. List of ADS terms is in the drop-down menu above
        the search bar at https://ui.adsabs.harvard.edu/

    query_mask : pd.DataFrame, dtype == boolean
        A boolean mask to select which sources should be queried.

    Returns
    -------
    queries : pd.DataFrame
        pandas DataFrame with query strings, ads articles objects
        retreived from the ADS, and bibcodes from those articles
        objects. queries index corresponds to sources index.

    bad_queries : list
        List of index values from the sources DataFrame for which
        values in columns to be searched either contained spaces or
        were 'x'.
    '''
    queries = make_queries(sources, search_cols, ads_search_terms=ads_search_terms, query_mask=query_mask)
    queries.insert(len(queries.columns), 'ADSarticles', [None]*len(queries))
    queries.insert(len(queries.columns), 'bibcode', ['']*len(queries))
    valid_queries = queries.loc[queries['qstr'] != 'x']

    if len(valid_queries) == 0:
        print('query_bibcodes created no query strings')
        return ((queries))

    if confirm_ads_submission(valid_queries):

        #set up and start a progress bar
        widgets = ['Queries: ', progressbar.Percentage(),' ', progressbar.Bar(marker='='),'|', progressbar.Timer(),]
        bar = progressbar.ProgressBar(widgets=widgets, maxval=(len(valid_queries)-1)).start()
        qindex = list(valid_queries.index)

        for i in qindex:
            q = queries.loc[i, 'qstr']
            search = ads.SearchQuery(q=q, fl='bibcode')
            try:
                search.execute()
            finally:
                queries.to_json('queries.json')
            queries.loc[i, 'ADSarticles'] = search.articles
            queries.loc[i, 'bibcode'] = ' '.join(list(map(lambda x: x.bibcode, search.articles)))
            bar.update(qindex.index(i))

        bar.finish()

    return((queries))
'''
def query_ads(sources, search_cols, ads_fetch_terms, ads_search_terms=None, fetch_cols=None, query_mask=None, wrapper=None, article_processor=None):

    Submit API queries to NASA/ADS.

    Parameters
    sources : pd.DataFrame
        Papers for which bibcodes will be found on NASA/ADS.

    search_cols : list-like
        Column labels in the sources DataFrame which contain data with
        which to construct the ADS search queries.

    ads_fetch_terms : list-like
        ADS search terms to get for each query. This is a list of data
        to fields to download from the API rather than search terms
        with which to construct the search query from the sources
        DataFrame. List of ADS terms is in the drop-down menu above
        the search bar at https://ui.adsabs.harvard.edu/

    ads_search_terms:
        ADS search terms corrensponding to the column labels in
        search_cols. Length and order of terms in this list must
        correspond to length and order of columns listed in
        search_cols. If None, assume the column labels are ADS
        search terms. List of ADS terms is in the drop-down menu above
        the search bar at https://ui.adsabs.harvard.edu/

    fetch_cols : list-like
        Bibliography column labels corresponding to the ADS terms
        listed in ads_fetch_terms. This is a list of bib columns to
        populate with ADS data. Length and order of column labels in
        this list must correspond to the length and order of ADS terms
        in ads_fetch_terms.

    query_mask : pd.DataFrame, dtype == boolean
        A boolean mask to select which sources should be queried.

    wrapper : string
        An ADS operator to wrap the query string, such as
        'references'. List of ADS operators is in the drop-down menu
        above the search bar at https://ui.adsabs.harvard.edu/

    article_processor : function
        Function that takes an ads article object and returns a
        list-like object of with values for a bibliography entry. If
        not provided, assume ads_fetch_terms contains bibliography column
        labels and enter fetched data directly into the results
        DataFrame.

    Returns
    -------
    results : pd.DataFrame
        DataFrame whose columns are labeled in fetch_cols (or
        ads_fetch_terms if bibliography columns are ADS terms).
        Addiditionally contains a 'srcidx' column whose value is the
        index in the sources DataFrame corresponding to the source
        that generated this query result.

    queries : pd.DataFrame
        DataFrame with query strings and data returned from ADS
        queries. Index corresponds to the sources index.


    if type(search_cols) == str:
        search_cols = [search_cols]
    if type(ads_fetch_terms) == str:
        ads_fetch_terms = [ads_fetch_terms]
    if type(fetch_cols) == str:
        fetch_cols = [fetch_cols]

    print(search_cols)
    print(ads_search_terms, '\n')

    queries = make_queries(sources, search_cols, ads_search_terms=ads_search_terms, query_mask=query_mask, wrapper=wrapper)
    queries.insert(len(queries.columns), 'ADSarticles', [None]*len(queries))
    queries.insert(len(queries.columns), 'bibcode', ['']*len(queries))
    valid_queries = queries.loc[queries['qstr'] != 'x']

    print(valid_queries)

    if fetch_cols is None:
        fetch_cols = ads_fetch_terms

    fetch_cols.append('srcidx')
    results = pd.DataFrame(columns=fetch_cols)

    if len(valid_queries) == 0:
        print('query_bibcodes created no query strings')
        return ((queries))

    if article_processor is None:
        article_processor = lambda x: [x.__getattribute__(t) for t in ads_fetch_terms]

    submitter = lambda x: submit_ads_queries(x, valid_queries, results, fetch_cols, ads_fetch_terms, article_processor)

    if confirm_ads_submission(valid_queries):
        valid_queries.apply(submitter, axis=1)
    return(results, queries)
'''

def query_ads(sources, search_cols, fetch_cols, ads_dict=None, query_mask=None, wrapper=None, article_processor=None):
    '''
    Submit API queries to NASA/ADS.

    Parameters
    sources : pd.DataFrame
        Papers for which bibcodes will be found on NASA/ADS.

    search_cols : list-like
        Column labels in the sources DataFrame which contain data with
        which to construct the ADS search queries.

    fetch_cols : list-like
        Column labels in the sources DataFrame that should be populated with
        data from ADS.

    query_mask : pd.DataFrame, dtype == boolean
        A boolean mask to select which sources should be queried.

    wrapper : string
        An ADS operator to wrap the query string, such as
        'references'. List of ADS operators is in the drop-down menu
        above the search bar at https://ui.adsabs.harvard.edu/

    article_processor : function
        Function that takes an ads article object and returns a
        list-like object of with values for a bibliography entry. If
        not provided, assume ads_fetch_terms contains bibliography column
        labels and enter fetched data directly into the results
        DataFrame.

    Returns
    -------
    results : pd.DataFrame
        DataFrame whose columns are labeled in fetch_cols (or
        ads_fetch_terms if bibliography columns are ADS terms).
        Addiditionally contains a 'srcidx' column whose value is the
        index in the sources DataFrame corresponding to the source
        that generated this query result.

    queries : pd.DataFrame
        DataFrame with query strings and data returned from ADS
        queries. Index corresponds to the sources index.
    '''

    if type(search_cols) == str:
        search_cols = [search_cols]
    if type(fetch_cols) == str:
        fetch_cols = [fetch_cols]
    if ads_dict is not None:
        ads_fetch_terms = [ads_dict[c] for c in fetch_cols]
    else:
        ads_search_terms = fetch_cols

    results_cols = fetch_cols + ['srcidx']
    results = pd.DataFrame(columns=results_cols)

    queries = make_queries(sources, search_cols, ads_dict=ads_dict, query_mask=query_mask, wrapper=wrapper)
    queries.insert(len(queries.columns), 'ADSarticles', [None]*len(queries))
    queries.insert(len(queries.columns), 'bibcode', ['']*len(queries))
    valid_queries = queries.loc[queries['qstr'] != 'x']

    print(valid_queries)

    if len(valid_queries) == 0:
        print('query_bibcodes created no query strings')
        return (results, queries)

    if article_processor is None:
        article_processor = lambda x: [x.__getattribute__(t) for t in ads_fetch_terms]

    submitter = lambda x: submit_ads_queries(x, valid_queries, results, fetch_cols, ads_fetch_terms, article_processor)

    if confirm_ads_submission(valid_queries):
        valid_queries.apply(submitter, axis=1)
    return(results, queries)
