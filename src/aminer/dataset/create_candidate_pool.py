import sys

import logging
import random

import aminer.dataset.es_request as es_request
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
        "_source": ["id", "references", "abstract"],
        "size": 10000,
        "query": {"match": {"year": year}
                  }
    }, scroll='2m')

    id_list = list()
    scroll_id = res['_scroll_id']
    while res['hits']['hits'] and len(res['hits']['hits']) > 0:
        for hit in res['hits']['hits']:
            reference_list = hit['_source']['references']
            abstract = hit['_source']['abstract']

            if len(reference_list) >= num_citations:
                if abstract is not None and len(abstract) > 0:
                    if check_if_all_papers_have_valid_abstracts(reference_list):
                        id_list.append(hit['_source']['id'])

            if len(id_list) >= num_papers:
                break

            res = es.scroll(scroll_id=scroll_id, scroll='2m',
                            request_timeout=10)

    es.clear_scroll(body={'scroll_id': scroll_id})

    return id_list


def check_if_all_papers_have_valid_abstracts(paper_ids):
    abstracts = query_on_paper_id.get_abstract_by_pids(paper_ids)

    for _, abstract in abstracts.items():
        if abstract is None or len(abstract) == 0:
            return False

    return True


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
    abstract_dict = query_papers_by_year.get_abstract(list(abstract_set))
    return candidate_dict, abstract_dict


import pkg_resources
import os
def print_to_file(mylist, filename):
    support_directory = pkg_resources.resource_filename("aminer", "support") 
    filepath = os.path.join(support_directory, filename)
    with open(filepath, 'w') as f:
        for item in mylist:
            f.write("%s\n" % item)
    

if __name__ == "__main__":
    # id_list = ["5", "100008599", "100008278", "100007563"]

    id_list = query_papers_by_year(2019, 100, 20)

    print_to_file(id_list, '2019_recommendations.txt')

    # for id in id_list:
    #     print(query_on_paper_id[id])

    # candidate_dict, abstract_dict = get_candidate_dict(id_list, excluded_ids, 10)
    # print(candidate_dict)
    # print(len(candidate_dict))

    # print(abstract_dict)
    # print(len(abstract_dict))
