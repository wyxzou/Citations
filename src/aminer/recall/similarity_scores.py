"""
 - The query in get_fos_counter does not have the intended behaviour, but I'm not yet sure why (Maybe we require nested queries?). get_author_counter, and get_keyword_counter are also probably wrong.
 - What are some examples where the 'keywords' field is populated?, because I haven't found any yet.
"""

import logging
from heapq import nlargest
import collections

import src.aminer.dataset.es_request as es_request

"""
Not working yet. 
- It should be possible to get the highest ranking items in a single query. The script can't access the values stored inside elasticsearch.
- I tried to set some fields to 'store' in the mapping, and then recreate the index, but I haven't been able to get it to work yet. 
- Possibly relevant links:
 https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-script-score-query.html
 https://www.elastic.co/guide/en/elasticsearch/reference/master/modules-scripting.html
 https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-store.html
"""
def similarity_scores_in_one_query(id, author_coeff, fos_coeff, keyword_coeff):
    id_set = set()
    logging.basicConfig(level=logging.ERROR)
    es = es_request.connect_elasticsearch()

    id_res = find_by_id(id)
    author_list = find_author_list(id_res)
    fos_list = find_fos_list(id_res)
    keyword_list = find_keyword_list(id_res)

    res = es.search(index="aminer", body={
        "_source": ["id"],
        "size": 5000,
        "query": {
            "function_score": {
                "query": {
                    "match_all": {}
                }
            }
        },
        "sort": {
            "_script": {
                "type": "number",
                "script": {
                    "lang": "painless",
                    "source": """

                    def author_set1 = new HashSet(); 
                    for (int i = 0; i < params.author_list.length; ++i) {
                        author_set1.add(params.author_list[i]);
                    }

                    def fos_set1 = new HashSet(); 
                    for (int i = 0; i < params.fos_list.length; ++i) {
                        fos_set1.add(params.fos_list[i]);
                    }

                    def keyword_set1 = new HashSet(); 
                    for (int i = 0; i < params.keyword_list.length; ++i) {
                        keyword_set1.add(params.keyword_list[i]);
                    }

                    def author_set2 = new HashSet(); 
                    def fos_set2 = new HashSet(); 
                    def keyword_set2 = new HashSet(); 

                    author_set1.retainAll(author_set2);
                    fos_set1.retainAll(fos_set2);
                    keyword_set1.retainAll(keyword_set2);

                    int author_matches = author_set1.size(); 
                    int fos_matches = fos_set1.size();  
                    int keyword_matches =  keyword_set1.size();                     
                    _score + author_matches * params.author_coeff  + fos_matches * params.fos_coeff + keyword_matches * params.keyword_coeff;
                    """,
                    "params": {
                        "author_coeff": author_coeff,
                        "fos_coeff": fos_coeff,
                        "keyword_coeff": keyword_coeff,
                        "author_list": author_list,
                        "fos_list": fos_list,
                        "keyword_list": keyword_list
                    }
                },
                "order": "asc"
            }
        }
    }, scroll='2m')

    return res


# Not used. Function calculates the score between two ids.
def individual_score_calculation(id_1, id_2, author_coeff, fos_coeff, keyword_coeff):
    id_1_res = find_by_id(id_1)
    id_2_res = find_by_id(id_2)
    author_matches = 0
    fos_matches = 0
    keyword_matches = 0

    if id_1_res["hits"]["total"]["value"] == 0:
        print("Did not find id 1")
        return
    else:
        id_1_source = id_1_res["hits"]["hits"][0]['_source']
        id_1_authors = id_1_source["authors"]
        id_1_fos = id_1_source["fos"]
        id_1_keywords = id_1_source["keywords"]

    if id_2_res["hits"]["total"]["value"] == 0:
        print("Did not find id 2")
        return
    else:
        id_2_source = id_2_res["hits"]["hits"][0]['_source']
        id_2_authors = id_2_source["authors"]
        id_2_fos = id_2_source["fos"]
        id_2_keywords = id_2_source["keywords"]

    id_1_authors_set = set()
    id_1_fos_set = set()
    id_1_keywords_set = set(id_1_keywords)

    for author in id_1_authors:
        id_1_authors_set.add(author["id"])

    for author in id_2_authors:
        if author["id"] in id_1_authors_set:
            author_matches += 1

    for fos in id_1_fos:
        id_1_fos_set.add(fos["name"])

    for fos in id_2_fos:
        if fos["name"] in id_1_fos_set:
            fos_matches += 1

    for keyword in id_2_keywords:
        if keyword in id_1_keywords_set:
            keyword_matches += 1

    print(str(author_matches) + " " + str(fos_matches) + " " + str(keyword_matches))
    total_score = author_coeff * author_matches + fos_coeff * fos_matches + keyword_coeff * keyword_matches
    return total_score


def get_author_counter(author_list):
    author_counter = collections.Counter()

    logging.basicConfig(level=logging.ERROR)
    es = es_request.connect_elasticsearch()
    for author in author_list:
        id_set = set()
        res = es.search(index="aminer", body={
            "_source": ["id", "authors.id"],
            "size": 5000,
            "query": {
                "bool": {
                    "must": [
                        {"match": {"authors.id": author}}
                    ]
                }
            }
        }, scroll='2m')

        while res['hits']['hits'] and len(res['hits']['hits']) > 0:
            for hit in res['hits']['hits']:

                retrieved_author_list = [d['id'] for d in hit['_source']['authors']]
                if author in retrieved_author_list:
                    id_set.add(hit['_source']['id'])

            # get es scroll id
            scroll_id = res['_scroll_id']
            # use es scroll api
            res = es.scroll(scroll_id=scroll_id, scroll='2m',
                            request_timeout=10)
        author_counter.update(list(id_set))
    return author_counter


def get_fos_counter(fos_list):
    fos_counter = collections.Counter()

    logging.basicConfig(level=logging.ERROR)
    es = es_request.connect_elasticsearch()
    for fos in fos_list:
        id_set = set()
        res = es.search(index="aminer", body={
            "_source": ["id", "fos.name"],
            "size": 5000,
            "query": {
                "bool": {
                    "must": [
                        {"match": {"fos.name": fos}}
                        # Does not work as intended, I'm not yet sure how to do it properly (Maybe we require nested queries?). Returns some results where the specifed fos is not present. I'm not sure if the same problem exists for author and keyword.
                    ]
                }
            }
        }, scroll='2m')

        while res['hits']['hits'] and len(res['hits']['hits']) > 0:
            for hit in res['hits']['hits']:

                retrieved_fos_list = [d['name'] for d in hit['_source']['fos']]
                if fos in retrieved_fos_list:  # This if statement is required because the above query doesn't work as intended.
                    id_set.add(hit['_source']['id'])

            # get es scroll id
            scroll_id = res['_scroll_id']
            # use es scroll api
            res = es.scroll(scroll_id=scroll_id, scroll='2m',
                            request_timeout=10)
        fos_counter.update(list(id_set))
    return fos_counter


def get_keyword_counter(keyword_list):
    keyword_counter = collections.Counter()

    logging.basicConfig(level=logging.ERROR)
    es = es_request.connect_elasticsearch()
    for keyword in keyword_list:
        id_set = set()
        res = es.search(index="aminer", body={
            "_source": ["id", "keywords"],
            "size": 5000,
            "query": {
                "bool": {
                    "must": [
                        {"match": {"keyword": keyword}}
                    ]
                }
            }
        }, scroll='2m')

        while res['hits']['hits'] and len(res['hits']['hits']) > 0:
            for hit in res['hits']['hits']:

                retrieved_keyword_list = hit['_source']['keywords']
                if keyword in retrieved_keyword_list:
                    id_set.add(hit['_source']['id'])

            # get es scroll id
            scroll_id = res['_scroll_id']
            # use es scroll api
            res = es.scroll(scroll_id=scroll_id, scroll='2m',
                            request_timeout=10)
        keyword_counter.update(list(id_set))
    return keyword_counter


def find_by_id(id):
    # We should probably not be connecting each time.
    logging.basicConfig(level=logging.ERROR)
    es = es_request.connect_elasticsearch()
    res = es.search(index="aminer", body={"query": {"match": {"id": id}}})
    return res


def find_author_list(res):
    papers = res['hits']['hits']
    author_list = list()

    if len(papers) != 0:
        authors = papers[0]['_source']['authors']
        author_list = [d['id'] for d in authors if 'id' in d]

    return author_list


def find_fos_list(res):
    papers = res['hits']['hits']
    fos_list = list()

    if len(papers) != 0:
        fos = papers[0]['_source']['fos']
        fos_list = [d['name'] for d in fos if 'name' in d]

    return fos_list


def find_keyword_list(res):
    papers = res['hits']['hits']
    keyword_list = list()

    if len(papers) != 0:
        keyword_list = papers[0]['_source']['keywords']

    return keyword_list


# Returns a dict of scores for the given counters and coefficients
def calculate_scores(author_counter, fos_counter, keyword_counter, author_coeff, fos_coeff, keyword_coeff):
    score_dict = dict()

    for k in author_counter.keys():
        if k in score_dict.keys():
            score_dict[k] = score_dict[k] + author_counter[k] * author_coeff
        else:
            score_dict[k] = author_counter[k] * author_coeff

    for k in fos_counter.keys():
        if k in score_dict.keys():
            score_dict[k] = score_dict[k] + fos_counter[k] * fos_coeff
        else:
            score_dict[k] = fos_counter[k] * fos_coeff

    for k in keyword_counter.keys():
        if k in score_dict.keys():
            score_dict[k] = score_dict[k] + keyword_counter[k] * keyword_coeff
        else:
            score_dict[k] = keyword_counter[k] * keyword_coeff

    return score_dict


# Returns a list of the highest ranking papers
def get_highest_ranking_papers(id, author_coeff, fos_coeff, keyword_coeff, num_to_return):
    id_res = find_by_id(id)

    author_list = find_author_list(id_res)
    fos_list = find_fos_list(id_res)
    keyword_list = find_keyword_list(id_res)

    author_counter = get_author_counter(author_list)
    fos_counter = get_fos_counter(fos_list)
    keyword_counter = get_keyword_counter(keyword_list)

    score_dict = calculate_scores(author_counter, fos_counter, keyword_counter, author_coeff, fos_coeff, keyword_coeff)

    id_string = str(id)
    if id_string in score_dict.keys():
        del score_dict[id_string]

    highest_ranking_list = nlargest(num_to_return, score_dict, key=score_dict.get)

    return highest_ranking_list


# Example using id = 101132528
n_highest_ranking = get_highest_ranking_papers(101132528, 1, 1, 1, 1000)
print(n_highest_ranking)


## Not working yet. The script can't access the values stored inside elasticsearch.
# print(similarity_scores_in_one_query(100008278, 5.6, 2.1, 0.9))