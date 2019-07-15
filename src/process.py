import get_candidates
import find_year
import json

def read_citations():
    filename = "../output_data.json"
    with open(filename, "rt") as fp:
        content = json.load(fp)

    candidates = {}
    for c in content:
        for key in c:
            candidates[key] = c[key]

    return candidates



if __name__ == '__main__':
    content = []
    with open("../ids.txt") as f:
        content = f.readlines()
        # you may also want to remove whitespace characters like `\n` at the end of each line
        content = [x.strip() for x in content] 

    
    filename = "output_data.json"

    # id_list = find_year.find_year(2015, 5, 0)
    # content = id_list

    feeds = []
    ids_group = []
    for pid in content:
        candidate_dict, abstract_dict = get_candidates.get_candidate_dict([pid], [], 10)
        feeds.append(candidate_dict)

    with open(filename, mode='w', encoding='utf-8') as feedsjson:        
        json.dump(feeds, feedsjson)

