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


if __name__ == '__main__':
    find_year(2015, 100, 2)

