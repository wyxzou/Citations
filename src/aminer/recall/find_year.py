import sys
sys.path.append("/to/your/src/")

import logging
import random


import aminer.dataset.es_request as es_request


def find_year(year, num_papers, num_citations):
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


def get_abstract(paperids):
    """
    Gets the abstract of papers with the provided list of ids

    :param paperids: list[int]
        The list of paper ids to find the abstract for
    :return:
        Dictionary with paper id as key and abstract as value
    """
    es = es_request.connect_elasticsearch()

    dic = {}
    for paperid in paperids:
        res = es.search(index="aminer", body={"query": {"match": {"id": paperid}}})
        if not res["hits"]["hits"]:
            print("Error: no such id for ", paperid)
        else:
            dic[paperid] = res["hits"]["hits"][0]['_source']['abstract']

    return dic


if __name__ == '__main__':

    ids = find_year(2015, 5, 2)
    print(ids)
    print(get_abstract(ids))
