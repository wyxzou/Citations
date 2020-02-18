import pkg_resources
import os

from tqdm import tqdm

from aminer.recall.query_es import get_year_by_pid, get_references_by_pid
from aminer.precision.utility import dump_json

support_directory = pkg_resources.resource_filename("aminer", "support")

if __name__ == '__main__':
    file = os.path.join(support_directory, '2019_ids.txt')

    ids = [line.rstrip('\n') for line in open(file)]
    count = 0
    over_dict = {}
    for pid in tqdm(ids):
        references = get_references_by_pid(pid)
        over = 0
        for p in references:
            year = int(get_year_by_pid(p))
            if year > 2017:
                over += 1

        over_dict[pid] = (over, len(references))

    dump_json(os.path.join(support_directory, 'check_over.json'), over_dict)

    print(len(over_dict) / 100.0)
