import numpy as np
import os
import pkg_resources
import json

from aminer.recall.query_es import get_references_by_pid

def num_intersection(pred, truth):
    intersection = 0
    for document in pred:
        if document in truth:
            intersection += 1

    return intersection


def precision(pred, truth):
    cardinality_retrieved_documents = len(pred)
    intersection = num_intersection(pred, truth)

    return intersection/cardinality_retrieved_documents


def recall(pred, truth):
    cardinality_retrieved_documents = len(truth)
    intersection = num_intersection(pred, truth)

    return intersection / cardinality_retrieved_documents


if __name__ == '__main__':
    root_directory = pkg_resources.resource_filename("aminer", "support")

    outfile = os.path.join(root_directory, 'recommendations.json')

    with open(outfile, 'r') as f:
        recommendations = json.load(f)
        f.close()

    for pid, recs in recommendations.items():
        pred_reference_list = [e[0] for e in recs]
        true_reference_list = get_references_by_pid(pid)

        print(precision(pred_reference_list, true_reference_list))
        print(recall(pred_reference_list, true_reference_list))

