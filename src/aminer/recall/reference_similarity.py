import logging
from heapq import nlargest, heappush, heappushpop
# from scipy.stats import chi2_contingency

import aminer.dataset.es_request as es_request

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
    n22 = 4107340 - paper1_reference_count - paper2_reference_count

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

    q = []
    id_set = set([str(id)])

    logging.basicConfig(level=logging.ERROR)
    es = es_request.connect_elasticsearch()
    for reference in reference_list:

        res = es.search(index="aminer", body={
            "_source": ["id", "references"],
            "size": 5000,
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

                if not id in id_set:
                    retrieved_reference_list = hit['_source']['references']
                    number_of_shared_references = len(set(reference_list).intersection(retrieved_reference_list))
                    contingency_table = get_contingency_table(reference_list, retrieved_reference_list)
                    # chi2, p, dof, ex = chi2_contingency(contingency_table)
                    # print(str(chi2) + " " + str(p) + " " + str(dof) + " " + str(ex))

                    id_set.add(id)
                    if len(q) < candidate_set_size:
                        heappush(q, (number_of_shared_references, id))
                    else:
                        heappushpop(q, (number_of_shared_references, id))

            # use es scroll api
            res = es.scroll(scroll_id=scroll_id, scroll='2m',
                            request_timeout=10)

        es.clear_scroll(body={'scroll_id': scroll_id})

    return nlargest(candidate_set_size, q)

if __name__ == '__main__':
    candidate_set = get_candidate_set(2029880715, 100)
    print(candidate_set)

    # for paper in candidate_set:
    #     print_res(find_by_id(paper[1]))
