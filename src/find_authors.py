import es_request
import logging
import json


# ground truth and pool are a list of ids
def num_included(ground_truth, pool):
    count = 0
    for i in ground_truth:
        if i in pool:
            count += 1

    return count / len(ground_truth)


# given a paper, return a list of author ids associated with that paper
def find_authors(pid):
    logging.basicConfig(level=logging.ERROR)
    es = es_request.connect_elasticsearch()  

    res = es.search(index="aminer", body={"query": {"match" : {"id": pid}}})
    
    author_list = res['hits']['hits'][0]['_source']['authors']

    author_ids = []
    for author in author_list:

        author_ids.append(author['id'])

    return author_ids


# matching authors.name is not necessary, feel free to remove it
def find_papers_with_author_id(author_id):
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


# gets id given res
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
    with open('../ids.txt') as f:
        paper_ids = f.read().splitlines()

    # paper_ids = ['100008749']

    # r = find_papers_with_author_id('2702511795')
    # print(r['hits']['hits'][0])
    # print(r['hits']['hits'][1])
    # print(get_ids(r))
    
    filename = "../shared_authors.json"
    dic = {}
    for pid in paper_ids:
        author_ids = find_authors(pid)
        recommended = []
        for a_id in author_ids:
            res = find_papers_with_author_id(a_id) # 'Peter Kostelnik')
            papers_with_shared_author = get_ids(res)
            recommended.extend(list(set(papers_with_shared_author)))

        recommended = list(set(recommended))
        recommended.remove(pid)
        dic[pid] = recommended

    with open(filename, mode='w', encoding='utf-8') as feedsjson:        
        json.dump(dic, feedsjson)

    # print(if_references_exist('100008749'))