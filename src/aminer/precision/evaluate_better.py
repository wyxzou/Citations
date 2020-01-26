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
from gensim.models import FastText
from scipy import spatial
from tqdm import tqdm

from aminer.recall.query_es import get_abstract_by_pids, get_references_by_pid, get_fos_by_pid, get_lang_by_pid
from aminer.precision.metrics import recall, precision

root_directory = pkg_resources.resource_filename("aminer", "support")
model_directory = os.path.join(root_directory, "models")

vec = pickle.load(open(os.path.join(model_directory, 'vec.model'), 'rb'))
fasttext = FastText.load(os.path.join(model_directory, 'fasttext', 'fasttext.model'))
word_to_idx = list(vec.get_feature_names())


def compute_abstract_embedding(abstract, is_query=False):
    tfidf_vec = vec.transform([abstract])
    tfidf_vec = scipy.sparse.coo_matrix(tfidf_vec)
    word_count = 0
    sum_embedding = np.zeros(50)
    for _, word_index, word_tfidf in zip(tfidf_vec.row, tfidf_vec.col, tfidf_vec.data):
        word = word_to_idx[word_index]
        if word in fasttext.wv.vocab:
            word_count += 1
            if not is_query:
                sum_embedding += word_tfidf * fasttext.wv[word]
            else:
                sum_embedding += fasttext.wv[word]

    if word_count == 0:
        return [0] * 50

    return (sum_embedding / word_count).tolist()


def get_similarities(dataset_embedding, test_embeddings):
    # TODO: find a way to do this in one line
    similarities = []
    for embedding in test_embeddings:
        similarities += [1 - spatial.distance.cosine(embedding, dataset_embedding)]

    return similarities


def extract_fos_set(pid, filtered_fields=['mathematics', 'computer science', 'artificial intelligence']):
    foss = get_fos_by_pid(pid)
    fos_set = set()

    for fos in foss:
        fos_element = fos['name'].lower()
        if fos_element not in filtered_fields:
            fos_set.add(fos_element)

    return fos_set


def recommend(ids, k=100):
    ids_to_abstract = get_abstract_by_pids(ids)
    ids_to_embeddings = {id:compute_abstract_embedding(ids_to_abstract[id]) for id in ids_to_abstract}
    test_embeddings = [ids_to_embeddings[id] for id in ids_to_embeddings]
    recommendations = {id: [] for id in ids_to_embeddings}

    embeddings_directory = os.path.join(root_directory, "output_embeddings2")
    files = [os.path.join(embeddings_directory, f) for f in listdir(embeddings_directory) if isfile(os.path.join(embeddings_directory, f))]
    for file in tqdm(files):
        with open(file, 'r') as f:
            embeddings = json.load(f)
            f.close()

        for id, emb in embeddings.items():
            fos = extract_fos_set(id)
            similarities = get_similarities(emb, test_embeddings)
            for example_id, similarity in zip(list(ids_to_embeddings.keys()), similarities):
                target_fos = extract_fos_set(example_id)
                if not fos.isdisjoint(target_fos):
                    lang = get_lang_by_pid(example_id)
                    if lang is 'en' or lang is 'Not inputted':
                        if len(recommendations[example_id]) < k:
                            # grow the heap
                            heapq.heappush(recommendations[example_id], tuple([similarity, id]))
                        else:
                            # maintain the size
                            heapq.heappushpop(recommendations[example_id], tuple([similarity, id]))

    # discard similarities and only keep id from the tuples
    recommendations = {id: [a[1] for a in recommendations[id]] for id in recommendations}
    return recommendations


if __name__ == '__main__':
    file = os.path.join(root_directory, 'ids.txt')
    ids = [line.rstrip('\n') for line in open(file)]

    recommendations = recommend(ids, 100000)

    outfile = 'test_recommendations.json'

    with open(outfile, 'w') as f:
        json.dump(recommendations, f)

    for pid, recs in recommendations.items():
        pred_reference_list = [e[0] for e in recs]
        true_reference_list = get_references_by_pid(pid)

        print(precision(pred_reference_list, true_reference_list))
        print(recall(pred_reference_list, true_reference_list))


