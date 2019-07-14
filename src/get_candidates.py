import es_request
import find_year
import logging
import random
import sys

def get_references(paper_id):
    logging.basicConfig(level=logging.ERROR)
    es = es_request.connect_elasticsearch()
    res = es.search(index="aminer", body={"query": {"match": {"id": paper_id}}})

    if len(res['hits']['hits']) == 0:
        return list()

    reference_list = res['hits']['hits'][0]['_source']['references']
    return reference_list

def get_random_id():
    logging.basicConfig(level=logging.ERROR)
    es = es_request.connect_elasticsearch()
    res = es.search(index="aminer", body={
        "_source": ["id"],
        "size": 1,
        "query": {
            "function_score": {
                "random_score": {
                    "seed": random.randint(0, sys.maxsize)
                }
            }
        }
    })

    if len(res['hits']['hits']) == 0:
        return None

    id = res['hits']['hits'][0]['_source']['id']
    return id


def get_candidate_set(id, excluded_id_list, candidate_size):
    reference_id_list = get_references(id)
    citation_id_list = list()

    for id in reference_id_list:
        if id not in excluded_id_list:
            citation_id_list.append(id)

    if len(citation_id_list) > candidate_size:
        del citation_id_list[candidate_size:]
        return citation_id_list

    while len(citation_id_list) < candidate_size:
        id = get_random_id()
        if id in excluded_id_list:
            continue
        citation_id_list.append(id)

    return citation_id_list


def get_candidate_dict(id_list, excluded_id_list, candidate_size):
    key_and_value_ids = id_list

    candidate_dict = dict()
    for id in id_list:
        candidates = get_candidate_set(id, excluded_id_list, candidate_size)
        candidate_dict[id] = candidates

    for key, value in candidate_dict.items():
        temp = [key, value]
        key_and_value_ids.append(temp)

    abstract_dict = find_year.get_abstract(key_and_value_ids)
    return candidate_dict, abstract_dict


excluded_ids = ["2123991323", "23142202", "1483005138"]
id_list = ["5", "100008599", "100008278", "100007563"]
#id_list = find_year.find_year(2009, 1, 0)

candidate_dict, abstract_dict = get_candidate_dict(id_list, excluded_ids, 10)
print(candidate_dict)
print(abstract_dict)