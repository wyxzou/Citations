import sys
sys.path.append("/to/your/src/")

import logging
import random

import aminer.dataset.es_request as es_request
import aminer.dataset.create_candidate_pool as find_year
import aminer.recall.query_es as query_on_paper_id


def query_papers_by_year(year, num_papers, num_citations):
    """
    Finds a list of paper ids based on year criteria

    :param year: int
        which year we want our papers from
    :param num_papers:
        number of papers we want to find
    :param num_citations:
        minimum number of citations we want each paper to have
    :return:
        an array of paper ids with satisfied conditions
    """
    logging.basicConfig(level=logging.ERROR)
    es = es_request.connect_elasticsearch()
    res = es.search(index="aminer", body={
        "_source": ["id", "references"],
        "size": 10000,
        "query": {"match": {"year": year}
                  }
    })

    if len(res['hits']['hits']) == 0:
        return None

    id_list = list()
    updated = res['hits']['hits']
    random.shuffle(updated)
    for i in range(0, len(updated)):
        if len(id_list) >= num_papers:
            break

        reference_count = 0
        if len(res['hits']['hits']) == 0:
            reference_count = 0
        else:
            reference_list = updated[0]['_source']['references']
            reference_count = len(reference_list)

        if reference_count >= num_citations:
            id_list.append(updated[i]['_source']['id'])

    return id_list


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
    reference_id_list = query_on_paper_id.get_references_by_pid(id)

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
    abstract_set = set()

    candidate_dict = dict()
    for pid in id_list:
        candidates = get_candidate_set(pid, excluded_id_list, candidate_size)
        candidate_dict[pid] = candidates

    for key, value in candidate_dict.items():

        abstract_set.add(key)
        for v in value:
            abstract_set.add(v)

    # print(key_and_value_ids)
    abstract_dict = find_year.get_abstract(list(abstract_set))
    return candidate_dict, abstract_dict


if __name__ == "__main__":
    excluded_ids = ["2123991323", "23142202", "1483005138"]
    # id_list = ["5", "100008599", "100008278", "100007563"]

    id_list = find_year.find_year(2009, 5, 0)

    candidate_dict, abstract_dict = get_candidate_dict(id_list, excluded_ids, 10)
    print(candidate_dict)
    print(len(candidate_dict))

    # print(abstract_dict)
    print(len(abstract_dict))
