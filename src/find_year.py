import es_request
import logging
import random

# year: papers from which year we want
# num_papers: number of papers we want to find
# num_citations: the minimum number of citations we want each pulled paper to have
# returns: a array of paper ids to pull
def find_year(year, num_papers, num_citations):
    es = es_request.connect_elasticsearch()
    res = es.search(index="aminer", body={"query": {"match" : {"year": year}}})
    # print(res)
    chosen = []
    n = num_papers
    while n > 0:
        index = random.randint(0, len(res) - 1)

        ref = res["hits"]["hits"][index]['_source']['references']
        if len(ref) > num_citations-1:
            chosen.append(res["hits"]["hits"].pop(index)['_source']['id'])
            n -= 1

    return chosen


def get_abstract(paperids):
    es = es_request.connect_elasticsearch()

    dic = {}
    for paperid in paperids:
        res = es.search(index="aminer", body={"query": {"match" : {"id": paperid}}})
        print(res)
        if not res["hits"]["hits"]:
            print("Error: no such id for ", paperid)
        else:
            # print(res["hits"]["hits"][0]['_source'].keys())
            dic[paperid] = res["hits"]["hits"][0]['_source']['indexed_abstract']
    
    return dic


if __name__ == '__main__':
    ids = find_year(2015, 5, 2)
    print(get_abstract(ids))

