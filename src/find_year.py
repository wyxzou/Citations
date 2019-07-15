import es_request
import logging
import random
import sys

# year: papers from which year we want
# num_papers: number of papers we want to find
# num_citations: the minimum number of citations we want each pulled paper to have
# returns: a array of paper ids to pull
def find_year(year, num_papers, num_citations):
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