import json
import random
import pkg_resources
import os

def create_smaller_test_set(filename, k):
    root_directory = pkg_resources.resource_filename("aminer", "support")

    filepath = os.path.join(root_directory, filename)

    with open(filepath, 'r') as f:
        total_dict = json.load(f)
        f.close()

    subset_dict = {}

    for i in range(k):
        (pid, value) = total_dict.popitem()
        subset_dict[pid] = value

    with open(os.path.join(root_directory, 'samples.json'), 'w') as f:
        json.dump(subset_dict, f)
        f.close()


if __name__ == '__main__':
    create_smaller_test_set('id_to_embedding_dict_0.json', 100)






