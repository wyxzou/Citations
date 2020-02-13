import heapq
import json
import math
import os
import pickle
from os import listdir
from os.path import isfile

import numpy as np
import pkg_resources
import scipy
from gensim.models import FastText, Word2Vec
from scipy import spatial
from tqdm import tqdm

from aminer.recall.query_es import get_abstract_by_pids, get_references_by_pid
from aminer.precision.metrics import recall, precision

root_directory = pkg_resources.resource_filename("aminer", "support")
model_directory = os.path.join(root_directory, "models")

vec = pickle.load(open(os.path.join(model_directory, 'vec.model'), 'rb'))
fasttext = Word2Vec.load('word2vec_100/word2vec_100.model')
word_to_idx = list(vec.get_feature_names())
relevant_ids = None
dim = 100
filter_method = 'fos_author_filtered.json'
with open(filter_method) as relevant_ids_file:
    relevant_ids = json.load(relevant_ids_file)

for id in relevant_ids:
    relevant_ids[id] = set(relevant_ids[id])

def compute_abstract_embedding(abstract, is_query=False):
    tfidf_vec = vec.transform([abstract])
    tfidf_vec = scipy.sparse.coo_matrix(tfidf_vec)
    word_count = 0
    sum_embedding = np.zeros(dim)
    for _, word_index, word_tfidf in zip(tfidf_vec.row, tfidf_vec.col, tfidf_vec.data):
        word = word_to_idx[word_index]
        if word in fasttext.wv.vocab:
            word_count += 1
            if not is_query:
                sum_embedding += word_tfidf * fasttext.wv[word]
            else:
                sum_embedding += fasttext.wv[word]

    if word_count == 0:
        return [0] * dim

    return (sum_embedding / word_count).tolist()


def get_all_embeddings(test_id, current_embedding, files):
    for file in tqdm(files):
        with open(file, 'r') as f:
            embeddings = json.load(f)
            f.close()

        for id, emb in embeddings.items():
            if str(id) not in relevant_ids[test_id]:
                yield (id, -1)
            else:
                cosine_similarity = 1 - spatial.distance.cosine(current_embedding, emb)
                if not math.isnan(cosine_similarity):
                    yield (id, cosine_similarity)


def recommend(ids, k=100):
    ids_to_abstract = get_abstract_by_pids(ids)

    recommendations = {}

    for i, (target_id, abstract) in enumerate(ids_to_abstract.items()):
        print(i, "Finding recommendations for: ", target_id)
        embeddings_directory = 'word2vec_100_abstract_dictionaries'

        current_embedding = compute_abstract_embedding(abstract, is_query=True)

        files = [os.path.join(embeddings_directory, f) for f in listdir(embeddings_directory) if isfile(os.path.join(embeddings_directory, f))]

        all_embeddings = get_all_embeddings(target_id, current_embedding, files)
        recommendations[target_id] = heapq.nlargest(k, all_embeddings, key=lambda e: e[1])

    return recommendations


if __name__ == '__main__':
    file = os.path.join(root_directory, 'ids.txt')
    ids = [line.rstrip('\n') for line in open(file)]

    #recommendations = recommend(ids, 100000)

    outfile = os.path.join('r_word2vec_100_fos_author.json')
    recommendations = {}
    with open(outfile, 'r') as f:
        #json.dump(recommendations, f)
        recommendations = json.load(f)

    rec = 0
    for pid, recs in recommendations.items():
        pred_reference_list = [e[0] for e in recs[:50]]
        true_reference_list = get_references_by_pid(pid)

        print(precision(pred_reference_list, true_reference_list))
        example_rec = recall(pred_reference_list, true_reference_list)
        rec += example_rec
        print(example_rec)

    print('Overall Recall: ', rec / len(recommendations))







