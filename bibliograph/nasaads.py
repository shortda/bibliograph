import ads
import progressbar
from datetime import datetime
import pandas as pd

def make_queries(sources, search_cols, ads_terms=None, query_mask=None, wrapper=None):
	'''
	Make strings that represent ADS search queries

	Parameters
	----------
	sources : pd.DataFrame
		Papers for which data will be found on NASA/ADS.

	search_cols : list-like
		Column labels in the sources DataFrame which contain data for
		the ADS search queries.

	ads_terms:
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
	'''
	if query_mask is not None:
		sources = sources[query_mask]

	if ads_terms is not None:
		fields = [[c, ads_terms[i]] for i,c in enumerate(search_cols)]
	else:		
		fields = [[c, c] for i,c in enumerate(search_cols)]

	queries = pd.DataFrame(columns=['query'])
	bad_queries = []

	if len(queries) == 0:
		return((queries, bad_queries))

	for i in sources.index:
		query = []
		
		for f in fields:
			value = str(sources.loc[i][f[0]])
			if (' ' not in value) and (value != 'x'):
				query.append(f[1] + ':' + value)
			else:
				bad_queries.append(i)
				query.append('x')
				continue

		if 'x' not in query:
			query = ' '.join(query)
			if wrapper is not None:
				query = wrapper + '(' + query + ')'
		
			queries.loc[i, 'query'] = query

	return((queries, bad_queries))

def confirm_ads_submission(queries):
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
	numQueries = len(queries.index)
	q = ads.SearchQuery(q='q').execute()
	limits = ads.RateLimits('SearchQuery').limits
	reset = datetime.utcfromtimestamp(int(limits['reset'])).strftime('%H:%M:%S, %Y-%m-%d')
	if numQueries > int(limits['limit']):
		raise ValueError('\nTrying to run up to ' + str(numQueries) + ' NASA/ADS search queries but the daily limit is ' + limits['limit'] + '\n')
	elif numQueries > int(limits['remaining']):
		raise ValueError('\nTrying to run up to ' + str(numQueries) + ' NASA/ADS search queries but this API token only has ' + limits['remaining'] + ' queries remaining today.\n\tRate limit resets at ' + reset + '\n')
	else:
		remainder = int(limits['remaining']) - numQueries
		print('\nAbout to run ' + str(numQueries) + ' NASA/ADS search queries.\nThere will be ' + str(remainder) + ' queries available today after this operation.\nRate limit resets at ' + reset + '\n')
		if remainder <= (int(limits['limit'])*0.1):
			answer = input('Remainder will likely be less than 10% of the daily limit. Enter y to continue, anything else to break.\n')
			if (answer != 'y') and (answer != 'Y'):
				return(True)
			else:
				return(False)
		return(True)

def queryADSbibcodes(sources, search_cols, ads_terms=None, query_mask=None):
	'''
	Get ADS bibcodes for papers in the sources DataFrame

	Parameters
	----------
	sources : pd.DataFrame
		Papers for which bibcodes will be found on NASA/ADS.

	search_cols : list-like
		Column labels in the sources DataFrame which contain data for
		the ADS search queries.

	ads_terms:
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
	queries, bad_queries = make_queries(sources, search_cols, ads_terms=ads_terms, query_mask=query_mask)
	queries.insert(len(queries.columns), 'ADSarticles', [None]*len(queries))
	queries.insert(len(queries.columns), 'bibcode', ['']*len(queries))

	if len(queries) == 0:
		print('queryADSbibcodes created no query strings')
		return ((queries, bad_queries))

	if confirm_ads_submission(queries):

		#set up and start a progress bar
		widgets = ['Queries: ', progressbar.Percentage(),' ', progressbar.Bar(marker='='),'|', progressbar.Timer(),]
		bar = progressbar.ProgressBar(widgets=widgets, maxval=(len(queries)-1)).start()
		qIndex = list(queries.index)

		for i in qIndex:
			q = queries.loc[i, 'query']
			search = ads.SearchQuery(q=q, fl='bibcode')
			try:
				search.execute()
			finally:
				queries.to_json('queries.json')
			queries.loc[i, 'ADSarticles'] = search.articles
			queries.loc[i, 'bibcode'] = ' '.join(list(map(lambda x: x.bibcode, search.articles)))
			bar.update(qIndex.index(i))

		bar.finish()

	return((queries, bad_queries))

def queryADS(sources, search_cols, fetchTerms, ads_terms=None, fetchColumns=None, query_mask=None, wrapper='references', articleProcessor=None):
	'''
	Submit API queries to NASA/ADS.

	Parameters
	sources : pd.DataFrame
		Papers for which bibcodes will be found on NASA/ADS.

	search_cols : list-like
		Column labels in the sources DataFrame which contain data for
		the ADS search queries.

	fetchTerms : list-like
		ADS search terms to get for each query. This is a list of data
		to fields to download from the API rather than search terms
		with which to construct the search query from the sources
		DataFrame. List of ADS terms is in the drop-down menu above
		the search bar at https://ui.adsabs.harvard.edu/

	ads_terms:
		ADS search terms corrensponding to the column labels in 
		search_cols. Length and order of terms in this list must
		correspond to length and order of columns listed in
		search_cols. If None, assume the column labels are ADS
		search terms. List of ADS terms is in the drop-down menu above
		the search bar at https://ui.adsabs.harvard.edu/

	fetchColumns : list-like
		Bibliography column labels corresponding to the ADS terms
		listed in fetchTerms. This is a list of bib columns to
		populate with ADS data. Length and order of column labels in
		this list must correspond to the length and order of ADS terms
		in fetchTerms.

	query_mask : pd.DataFrame, dtype == boolean
		A boolean mask to select which sources should be queried.

	wrapper : string
		An ADS operator to wrap the query string, such as 
		'references'. List of ADS operators is in the drop-down menu
		above the search bar at https://ui.adsabs.harvard.edu/

	articleProcessor : function
		Function that takes an ads article object and returns a
		list-like object of with values for a bibliography entry. If
		not provided, assume fetchTerms contains bibliography column
		labels and enter fetched data directly into the results
		DataFrame.

	Returns
	-------
	results : pd.DataFrame
		DataFrame whose columns are labeled in fetchColumns (or
		fetchTerms if bibliography columns are ADS terms).
		Addiditionally contains a 'srcidx' column whose value is the
		index in the sources DataFrame corresponding to the source
		that generated this query result.

	queries : pd.DataFrame
		DataFrame with query strings and data returned from ADS
		queries. Index corresponds to the sources index.

	bad_queries : list
		List of index values from the sources DataFrame for which
		values in columns to be searched either contained spaces or 
		were 'x'.
	'''

	if type(search_cols) == str:
		search_cols = [search_cols]
	if type(fetchTerms) == str:
		fetchTerms = [fetchTerms]
	if type(ads_terms) == str:
		ads_terms = [ads_terms]
	if type(fetchColumns) == str:
		fetchColumns = [fetchColumns]

	queries, bad_queries = make_queries(sources, search_cols, ads_terms=ads_terms, query_mask=query_mask, wrapper=wrapper)
	queries.insert(len(queries.columns), 'ADSarticles', [None]*len(queries))

	if fetchColumns is None:
		theseColumns = fetchTerms
	else:
		theseColumns = fetchColumns

	theseColumns.append('srcidx')

	results = pd.DataFrame(columns=theseColumns)

	if len(queries) == 0:
		print('queryADS created no query strings')
		return((results, queries, bad_queries))

	if articleProcessor is None:
		articleProcessor = lambda x: [x.__getattribute__(t) for t in fetchTerms]

	if confirm_ads_submission(queries):

		#set up and start a progress bar
		widgets = ['Queries: ', progressbar.Percentage(),' ', progressbar.Bar(marker='='),'|', progressbar.Timer(),]
		bar = progressbar.ProgressBar(widgets=widgets, maxval=(len(queries)-1)).start()
		qIndex = list(queries.index)

		for i in qIndex:
			q = queries.loc[i, 'query']
			search = ads.SearchQuery(q=q, fl=fetchTerms)
			try:
				search.execute()
			except:
				results.to_json('results.json')
				queries.to_json('queries.json')
				raise
			queries.loc[i, 'ADSarticles'] = search.articles
			for article in search.articles:
				values = articleProcessor(article)
				values.append(i)
				results = results.append(pd.Series(dict(zip(theseColumns, values))), ignore_index=True)
			bar.update(qIndex.index(i))

		bar.finish()

	return((results, queries, bad_queries))
