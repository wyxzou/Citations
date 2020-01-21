import numpy as np
import os
import pkg_resources
import json

from aminer.recall.query_es import get_references_by_pid, \
    get_abstract_by_pids,\
    get_title_by_pid


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


def inspect_abstracts(pid):
    root_directory = pkg_resources.resource_filename("aminer", "support")

    outfile = os.path.join(root_directory, 'recommendations.json')
    inspect_directory = os.path.join(root_directory, 'inspect_abstracts')
    output_file = os.path.join(inspect_directory, str(pid) + '.txt')

    with open(outfile, 'r') as f:
        recommendations = json.load(f)
        f.close()

    target_abstract = get_abstract_by_pids([pid])
    print('target abstracts')
    for pid, abstract in target_abstract.items():
        title = get_title_by_pid(pid)
        with open(output_file, 'w') as f:
            f.write('Title: ' + title + '\n')
            f.write('Abstract: ' + abstract + '\n')
            f.write('\n')

    references = recommendations[str(pid)]
    references = [e[0] for e in references]
    abstracts = get_abstract_by_pids(references)
    print('recommended abstracts')

    for pid in references:
        abstract = abstracts[pid]
        title = get_title_by_pid(pid)
        with open(output_file, 'a') as f:
            f.write('Title: ' + title + '\n')
            f.write('Abstract: ' + abstract + '\n')
            f.write('\n')


if __name__ == '__main__':
    root_directory = pkg_resources.resource_filename("aminer", "support")

    outfile = os.path.join(root_directory, 'recommendations.json')

    with open(outfile, 'r') as f:
        recommendations = json.load(f)
        f.close()

    recalls = []
    for pid, recs in recommendations.items():
        pred_reference_list = [e[0] for e in recs]
        true_reference_list = get_references_by_pid(pid)

        print(precision(pred_reference_list, true_reference_list))
        r = recall(pred_reference_list, true_reference_list)
        print(r)
        recalls.append(r)

    print("Avg recall: ", sum(recalls)/len(recalls))

