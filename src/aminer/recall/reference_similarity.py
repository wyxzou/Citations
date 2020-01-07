import logging
import json
from heapq import nlargest, heappush, heappushpop

import aminer.dataset.es_request as es_request
import aminer.recall.functions as functions


def find_by_id(id):
    """
    Takes in an id and returns information about it queried from Elasticsearch.

    :param id: int
        the id of the search paper
    :return res: elasticsearch result
        Elasticsearch result that contains the information for the entered id
    """

    # We should probably not be connecting each time.
    logging.basicConfig(level=logging.ERROR)
    es = es_request.connect_elasticsearch()
    res = es.search(index="aminer", body={"query": {"match": {"id": id}}})
    return res


def print_res(res):
    """
        Prints the '_source' field from an Elasticsearch search result
    """

    if res['hits']['hits'] and len(res['hits']['hits']) > 0:
        print(res['hits']['hits'][0]['_source'])


def get_references(res):
    """
        Returns the ['_source']['references'] field from an Elasticsearhc search result
    """

    papers = res['hits']['hits']
    reference_list = list()

    if len(papers) != 0:
        reference_list = papers[0]['_source']['references']

    return reference_list


def get_contingency_table(paper1_references, paper2_references):
    """
    Returns a 2x2 contingency table
    """
    n11 = len(set(paper1_references).intersection(paper2_references))

    paper1_reference_count = len(paper1_references)
    paper2_reference_count = len(paper2_references)

    n12 = paper1_reference_count - n11
    n21 = paper2_reference_count - n11
    n22 = 4107340 - paper1_reference_count - paper2_reference_count - n11

    contingency_table = [[n11, n12],
                         [n21, n22]]
    return contingency_table


def get_candidate_set(id, candidate_set_size):
    """
    Returns a candidate set using reference similarity

    :param  id: int
        the id of the search paper
    :param  candidate_set_size: int
        candidate set size
    :return candidate_set: list of tuples
        list of tuples, where each tuple is: (score, id)
    """

    id_res = find_by_id(id)
    print_res(id_res)
    reference_list = get_references(id_res)

    arr = dict()

    q = []
    id_set = set([str(id)])

    logging.basicConfig(level=logging.ERROR)
    es = es_request.connect_elasticsearch()
    for reference in reference_list:

        res = es.search(index="aminer", body={
            "_source": ["id", "references"],
            "size": 10000,
            "query": {
                "bool": {
                    "must": [
                        {"match": {"references": reference}}
                    ]
                }
            }
        }, scroll='2m')

        # get es scroll id
        scroll_id = res['_scroll_id']

        while res['hits']['hits'] and len(res['hits']['hits']) > 0:
            for hit in res['hits']['hits']:
                id = hit['_source']['id']

                if id not in id_set:
                    retrieved_reference_list = hit['_source']['references']
                    number_of_shared_references = len(set(reference_list).intersection(retrieved_reference_list))
                    contingency_table = get_contingency_table(reference_list, retrieved_reference_list)
                    chi_square = functions.chi_square(contingency_table)
                    prob, absolute_error = functions.prob(chi_square)
                    id_set.add(id)

                    index = int(id)
                    print(index)
                    arr[index] = prob

                    if len(q) < candidate_set_size:
                        heappush(q, (prob, (id, number_of_shared_references)))
                    else:
                        heappushpop(q, (prob, (id, number_of_shared_references)))

            # use es scroll api
            res = es.scroll(scroll_id=scroll_id, scroll='2m',
                            request_timeout=10)

        es.clear_scroll(body={'scroll_id': scroll_id})

    print(arr)
    return nlargest(candidate_set_size, q)


def compute_score(similarity_dict, id_j):
    """
    :param  similarity_dict: dict {string, int}
        dictionary where the key is the id of the paper, and the value is the cosine similarity
    :param  id_j: string
        the id of a paper in AMiner
    :return score: int
        the score computed as defined in part III C of
        https://ieeexplore.ieee.org/document/7279056?fbclid=IwAR2YbsiF_aWB94AX_h413rlAfYqGHuBmEbusuYXSW4m1kW-eNIhxHMf1wFs
    """
    numerator = 0
    denominator = 0

    for id, similarity in similarity_dict.items():
        res = find_by_id(id)
        references = get_references(res)

        if id_j in references:
            numerator += similarity

        denominator += similarity

    return numerator / denominator


def get_reference_dict():
    """
    :return reference_dict: dict {string, list of strings}
        dictionary where the key is a paper id, and value is a list of its references (which are also paper id strings).
    """
    logging.basicConfig(level=logging.ERROR)
    es = es_request.connect_elasticsearch()

    reference_dict = dict()

    res = es.search(index="aminer", body={
        "_source": ["id", "references"],
        "size": 10000,
        "query": {
            "match_all": {}
        }
    }, scroll='2m')

    # get es scroll id
    scroll_id = res['_scroll_id']
    while res['hits']['hits'] and len(res['hits']['hits']) > 0:
        for hit in res['hits']['hits']:
            id = hit['_source']['id']

            if id not in reference_dict:
                reference_dict[id] = get_references(res)

        # use es scroll api
        res = es.scroll(scroll_id=scroll_id, scroll='2m',
                        request_timeout=10)
        print(len(reference_dict))

    es.clear_scroll(body={'scroll_id': scroll_id})
    return reference_dict


def output_index_dict(filename):
    """
    :param  filename: string
        filename of where the index dictionary should be outputted
    :return index_dict: dict {string, int}
        dictionary where key is paper id, and value is the vector index.
    """
    logging.basicConfig(level=logging.ERROR)
    es = es_request.connect_elasticsearch()

    id_set = set()

    res = es.search(index="aminer", body={
        "_source": ["id", "references"],
        "size": 10000,
        "query": {
            "match_all": {}
        }
    }, scroll='2m')

    # get es scroll id
    scroll_id = res['_scroll_id']
    while res['hits']['hits'] and len(res['hits']['hits']) > 0:
        for hit in res['hits']['hits']:
            id = hit['_source']['id']
            id_set.add(id)

        # use es scroll api
        res = es.scroll(scroll_id=scroll_id, scroll='2m',
                        request_timeout=10)
        print(len(id_set))

    es.clear_scroll(body={'scroll_id': scroll_id})

    id_sorted_list = sorted(id_set)

    index_dict = dict()
    cur_index = 0
    for id in id_sorted_list:
        index_dict[id] = cur_index
        cur_index += 1

    with open(filename, 'w') as fp:
        fp.write(json.dumps(index_dict))
    return index_dict


if __name__ == '__main__':
    # candidate_set = get_candidate_set(2029880715, 100)
    # print(candidate_set)

    # for paper in candidate_set:
    #     print_res(find_by_id(paper[1]))

    print(get_reference_dict())

    # output_index_dict("index.json")
