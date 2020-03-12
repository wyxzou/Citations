import pkg_resources
import json
import os
from aminer.recall.query_es import get_references_by_pid
from aminer.precision.metrics import recall, precision

if __name__ == '__main__':
    support_directory = pkg_resources.resource_filename("aminer", "support")

    outfile = os.path.join(support_directory, 'r_word2vec_150_fos_author.json')
    with open(outfile, 'w') as f:
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