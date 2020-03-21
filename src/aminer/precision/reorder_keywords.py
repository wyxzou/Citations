import pkg_resources

import os
from aminer.precision.utility import load_json, dump_json
from aminer.precision.metrics import recall, precision
from aminer.recall.query_es import get_references_by_pid

if __name__ == '__main__':
    support_directory = pkg_resources.resource_filename("aminer", "support")
    directory_2019 = os.path.join(support_directory, '2019_filters')
    sorted_candidate_set = load_json(os.path.join(directory_2019, 'sorted_candidate_sets.json'))
    aggregated_filters = load_json(os.path.join(directory_2019, 'aggregated_filters_o2_keywords.json'))

    rec = 0
    for pid, recs in sorted_candidate_set.items():
        counter = 0
        candidate_set = aggregated_filters[pid]
        sorted_candidate_set = []

        for id in recs:
            if id in candidate_set:
                sorted_candidate_set.append(id)
                counter += 1

                if counter == 10000:
                    break

        aggregated_filters[pid] = sorted_candidate_set
        true_reference_list = get_references_by_pid(pid)
        r = recall(sorted_candidate_set, true_reference_list)
        print(r)
        rec += r

    dump_json(os.path.join(directory_2019, 'aggregated_filters_o2_keywords_sorted_10000.json'), aggregated_filters))

    print('Overall Recall: ', rec/len(sorted_candidate_set))


