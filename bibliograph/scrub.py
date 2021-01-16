import string
import pandas as pd
from bibliograph.util import check_orphan
from bibliograph.util import count_true
from numpy import vectorize
from numpy import where
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer


def duplicated_nonspecial(series, new_specials=[]):
    specials = list(new_specials) + ['x', '?']
    nonspecial = series.apply(lambda x: x not in specials)
    return(series.duplicated() & nonspecial)


def report_specials(dataframe, new_specials=[]):
    specials = list(new_specials) + ['x', '?']
    output = {s:dataframe.apply(lambda x: x == s) for s in specials}
    output['counts'] = {s:output[s].apply(count_true, axis=1) for s in specials}
    return output


def report_duplicated(dataframe, new_specials=[]):
    specials = list(new_specials) + ['x', '?']
    dupes = dataframe.apply(duplicated_nonspecial, args=(new_specials))
    counts = dupes.apply(count_true, acis=1)
    return {'duplicated':dupes, 'counts':counts}


def cosine_similarity_matrices(df):
    vectorizers = {c:CountVectorizer(analyzer='char').fit_transform(df[c]) for c in df.columns}
    return {c:cosine_similarity(vectorizers[c].toarray()) for c in df.columns}


def get_similar_pair_indices(similarity_matrices, threshold, exclude_equivalent):
    if exclude_equivalent:
        test = vectorize(lambda x: (x > threshold) and (x < 0.999999999))
    else:
        test = vectorize(lambda x: (x > threshold))
    return {k:[p for p in zip(*where(test(v) == True)) if p[0] > p[1]] for k,v in similarity_matrices.items()}


def get_similar_pairs(df, threshold=0.9, exclude_equivalent=True):
    print('Generating cosine similarity matrices')
    similarity_matrices = cosine_similarity_matrices(df)
    print('Indexing similar values')
    index_pairs = get_similar_pair_indices(similarity_matrices, threshold, exclude_equivalent)
    print('Getting pairs of similar values')
    value_pairs = {k:[[df.loc[p[0], k], df.loc[p[1], k]] for p in v] for k,v in index_pairs.items()}
    return value_pairs
