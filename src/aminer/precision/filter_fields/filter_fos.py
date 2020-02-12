import json
import os
import pkg_resources
from tqdm import tqdm

from aminer.precision.utility import load_json, dump_json
from aminer.recall.query_es import get_abstract_by_pids, get_references_by_pid

support_directory = pkg_resources.resource_filename("aminer", "support")

def count_fos():
    dir = os.path.join(support_directory, 'fos_dictionaries')
    fos_counts = {}
    for i, filename in enumerate(os.listdir(dir)):
        id_to_fos = {}
        with open(os.path.join(dir, filename)) as jsonfile:
            id_to_fos = json.load(jsonfile)
            for id in id_to_fos:
                for fos in id_to_fos[id]:
                    if fos['name'] not in fos_counts:
                        fos_counts[fos['name']] = 0

                    fos_counts[fos['name']] += 1

        print('Batch: {}'.format(i))
        print('#FOS', len(fos_counts))
        dump_json(os.path.join(support_directory, 'output_fos_counts/fos_counts_{}.json'.format(i)), fos_counts)


fos_counts = load_json(os.path.join(support_directory, 'output_fos_counts/fos_counts_410.json'))
file = os.path.join('../support', '2019_ids.txt')
ids = [line.rstrip('\n') for line in open(file)]
threshold = 500000

test_recs = {}
for id in ids:
    test_recs[id] = get_references_by_pid(id)


all_relevant_ids = set(test_recs.keys())
for id in ids:
    all_relevant_ids = all_relevant_ids.union(test_recs[id])

dir = os.path.join(support_directory, 'fos_dictionaries')
relevant_id_to_fos = {}
files = os.listdir(dir)
for i, filename in tqdm(enumerate(files)):
    with open(os.path.join(dir, filename)) as jsonfile:
        id_to_fos = json.load(jsonfile)
        for id in id_to_fos:
            if id in all_relevant_ids:
                relevant_id_to_fos[id] = id_to_fos[id]

broad_fos = {a: fos_counts[a] for a in fos_counts.keys() if fos_counts[a] > threshold}

fos_counts = {}
all_recs = {id: [] for id in ids}
for i, filename in tqdm(enumerate(os.listdir(dir))):
    id_to_fos = {}
    with open(os.path.join(dir, filename)) as jsonfile:
        id_to_fos = json.load(jsonfile)
        for id in id_to_fos:
            for test_id in ids:
                test_fos = [a['name'] for a in relevant_id_to_fos[test_id] if a['name'] not in broad_fos]
                id_fos = [a['name'] for a in id_to_fos[id] if a['name'] not in broad_fos]
                if len(set(test_fos).intersection(set(id_fos))) > 0:
                    all_recs[test_id] += [id]

dump_json(os.path.join(support_directory, '2019_fos_filtered_recommendations.json'), all_recs)
