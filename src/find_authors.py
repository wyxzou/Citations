import es_request
import logging


# ground truth and pool are a list of ids
def num_included(ground_truth, pool):
    count = 0
    for i in ground_truth:
        if i in pool:
            count += 1

    return count / len(ground_truth)


# matching authors.name is not necessary, feel free to remove it
def find_author(author_id):
    logging.basicConfig(level=logging.ERROR)
    es = es_request.connect_elasticsearch()    

    res = es.search(index="aminer", body={
        "query": {
            "bool" : {
                "must": [
                    # { "match": {"authors.name": author_name} },
                    { "match": {"authors.id": author_id} }
                ]
            }
        }
    })

    return res


def get_ids(res):
    ids = []
    for doc in res['hits']['hits']:
        ids.append(doc['_source']['id'])

    return ids


def if_references_exist(paper_id):
    logging.basicConfig(level=logging.ERROR)
    es = es_request.connect_elasticsearch()   
    res = es.search(index="aminer", body={"query": {"match" : {"id": paper_id}}})
    
    references = res['hits']['hits'][0]['_source']['references']

    if not references:
        return 1

    count = 0
    for r in references:
        res = es.search(index="aminer", body={"query": {"match" : {"id": r}}})
        if not res['hits']['hits']:
            count += 1

    return (len(references) - count) / len(references)



if __name__ == '__main__':
    res = find_author('2702511795') # 'Peter Kostelnik')

    print(get_ids(res))
    print(if_references_exist('100008749'))



    # print(es.search(index="aminer", body={"query": {"match" : {"id": "ab"}}})['hits']['hits'])
