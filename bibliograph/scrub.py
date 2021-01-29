from bibliograph.util import count_true
from numpy import logical_and
from numpy import where
from pandas import DataFrame
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer


def duplicated_nonspecial(series, new_specials=[]):
    specials = list(new_specials) + ['x', '?']
    nonspecial = series.apply(lambda x: x not in specials)
    return(series.duplicated() & nonspecial)


def report_specials(df, new_specials=[]):
    specials = list(new_specials) + ['x', '?']
    output = {s:df.apply(lambda x: x == s) for s in specials}
    output['counts'] = {s:output[s].apply(count_true, axis=1) for s in specials}
    return output


def report_duplicated(df, new_specials=[]):
    specials = list(new_specials) + ['x', '?']
    dupes = df.apply(duplicated_nonspecial, args=(new_specials))
    counts = dupes.apply(count_true, acis=1)
    return {'duplicated':dupes, 'counts':counts}


def cosine_similarity_matrices(df):
    vectorizers = {c:CountVectorizer(analyzer='char').fit_transform(df[c]) for c in df.columns}
    return {c:cosine_similarity(vectorizers[c].toarray()) for c in df.columns}


def get_similar_pair_indices(similarity_matrices, threshold, exclude_equivalent):
    if exclude_equivalent:
        test = lambda x: logical_and((x > threshold), (x < 1))
    else:
        test = lambda x: (x > threshold)
    indexer = lambda x: [p for p in zip(*where(test(x) == True)) if p[0] > p[1]]
    return {k:indexer(v) for k,v in similarity_matrices.items()}


def get_similar_pairs(df, threshold=0.9, exclude_equivalent=True):
    print('Generating cosine similarity matrices')
    similarity_matrices = cosine_similarity_matrices(df)
    print('Indexing similar values')
    index_pairs = get_similar_pair_indices(similarity_matrices, threshold, exclude_equivalent)
    print('Getting pairs of similar values')
    value_pairs = {k:[[df.loc[p[0], k], df.loc[p[1], k]] for p in v] for k,v in index_pairs.items()}
    if exclude_equivalent:
        masks = {}
        for k,pairs in value_pairs.items():
            masks[k] = [pair[0]!=pair[1] for pair in pairs]
        value_pairs = {k:DataFrame(v)[masks[k]] for k,v in value_pairs.items()}
    else:
        value_pairs = {k:DataFrame(v) for k,v in value_pairs.items()}
    return value_pairs


def check_for_transpositions(s1, s2, up_to=1):
    if len(s1) == len(s2):
        if all([char in s1 for char in s2]):
            s1 = '|' + s1 + '|'
            s2 = '|' + s2 + '|'
            pairs1 = [s1[i:i+2] for i,c in enumerate(s1)]
            pairs2 = [s2[i:i+2] for i,c in enumerate(s2)]
            present = [p in pairs2 for p in pairs1]
            partition = [l for l in [present[i:i+3] for i in range(0, len(present), 3)] if len(l) == 3]
            if [not any(chunk) for chunk in partition].count(True) in range(1, up_to+1):
                is_transpose = []
                for i,chunk in enumerate(partition):
                    if not any(chunk):
                        if pairs2[i*3 + 1] == pairs1[i*3 + 1][::-1]:
                            is_transpose.append(True)
                        else:
                            is_transpose.append(False)
                if all(is_transpose):
                    return True
    return False
