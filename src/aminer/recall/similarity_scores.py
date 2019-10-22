"""
 - The query in get_fos_counter does not have the intended behaviour, but I'm not yet sure why (Maybe we require nested queries?). get_author_counter, and get_keyword_counter are also probably wrong.
 - What are some examples where the 'keywords' field is populated?, because I haven't found any yet.
"""

import logging
from heapq import nlargest
import collections

import es_request as es_request


def similarity_scores_in_one_query(id, author_coeff, fos_coeff, keyword_coeff):
    """
    Not working yet.
    - It should be possible to get the highest ranking items in a single query. The script can't access the values stored inside elasticsearch.
    - I tried to set some fields to 'store' in the mapping, and then recreate the index, but I haven't been able to get it to work yet.
    - Possibly relevant links:
     https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-script-score-query.html
     https://www.elastic.co/guide/en/elasticsearch/reference/master/modules-scripting.html
     https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-store.html

    :param id: int
        the id of the search paper
    :param author_coeff: float
        score multiplier for the number of authors
    :param fos_coeff: float
        score multiplier for the number of fos
    :param keyword_coeff: float
        score multiplier for the number of keywords
    :return res: elasticsearch result
        Elasticsearch result that contains the information for n highest scoring papers
    """

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


# # Not used. Function calculates the score between two ids.
# def individual_score_calculation(id_1, id_2, author_coeff, fos_coeff, keyword_coeff):
#     id_1_res = find_by_id(id_1)
#     id_2_res = find_by_id(id_2)
#     author_matches = 0
#     fos_matches = 0
#     keyword_matches = 0
#
#     if id_1_res["hits"]["total"]["value"] == 0:
#         print("Did not find id 1")
#         return
#     else:
#         id_1_source = id_1_res["hits"]["hits"][0]['_source']
#         id_1_authors = id_1_source["authors"]
#         id_1_fos = id_1_source["fos"]
#         id_1_keywords = id_1_source["keywords"]
#
#     if id_2_res["hits"]["total"]["value"] == 0:
#         print("Did not find id 2")
#         return
#     else:
#         id_2_source = id_2_res["hits"]["hits"][0]['_source']
#         id_2_authors = id_2_source["authors"]
#         id_2_fos = id_2_source["fos"]
#         id_2_keywords = id_2_source["keywords"]
#
#     id_1_authors_set = set()
#     id_1_fos_set = set()
#     id_1_keywords_set = set(id_1_keywords)
#
#     for author in id_1_authors:
#         id_1_authors_set.add(author["id"])
#
#     for author in id_2_authors:
#         if author["id"] in id_1_authors_set:
#             author_matches += 1
#
#     for fos in id_1_fos:
#         id_1_fos_set.add(fos["name"])
#
#     for fos in id_2_fos:
#         if fos["name"] in id_1_fos_set:
#             fos_matches += 1
#
#     for keyword in id_2_keywords:
#         if keyword in id_1_keywords_set:
#             keyword_matches += 1
#
#     print(str(author_matches) + " " + str(fos_matches) + " " + str(keyword_matches))
#     total_score = author_coeff * author_matches + fos_coeff * fos_matches + keyword_coeff * keyword_matches
#     return total_score


def get_author_counter(author_list):
    """
    Takes in a list of authors and returns a counter

    :param author_list: list of Strings
        list of author ids
    :return author_counter: Counter
        Counter where key=id, value=# of matching authors
    """

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
    """
    Takes in a list of fos and returns a counter

    :param fos_list: list of Strings
        list of fos names
    :return fos_counter: Counter
        Counter where key=id, value=# of matching fos
    """

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
    """
    Takes in a list of keywords and returns a counter

    :param keyword_list: list of Strings
        list of keywords
    :return keyword_counter: Counter
        Counter where key=id, value=# of matching keyword
    """

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
    """
    Takes in a list of keywords and returns a counter

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


def find_author_list(res):
    """
    Extracts the author list from the entered Elasticsearch result

    :param  res: Elasticsearch result
        Elasticsearch result that contains the information for a paper
    :return author_list: list of Strings
        list of author ids
    """

    papers = res['hits']['hits']
    author_list = list()

    if len(papers) != 0:
        authors = papers[0]['_source']['authors']
        author_list = [d['id'] for d in authors if 'id' in d]

    return author_list


def find_fos_list(res):
    """
    Extracts the fos list from the entered Elasticsearch result

    :param  res: Elasticsearch result
        Elasticsearch result that contains the information for a paper
    :return fos_list: list of Strings
        list of fos ids
    """

    papers = res['hits']['hits']
    fos_list = list()

    if len(papers) != 0:
        fos = papers[0]['_source']['fos']
        fos_list = [d['name'] for d in fos if 'name' in d]

    return fos_list


def find_keyword_list(res):
    """
    Extracts the keyword list from the entered Elasticsearch result

    :param  res: Elasticsearch result
        Elasticsearch result that contains the information for a paper
    :return keyword_list: list of Strings
        list of keyword ids
    """

    papers = res['hits']['hits']
    keyword_list = list()

    if len(papers) != 0:
        keyword_list = papers[0]['_source']['keywords']

    return keyword_list


def calculate_scores(author_counter, fos_counter, keyword_counter, author_coeff, fos_coeff, keyword_coeff):
    """
    Returns a dict of scores for the given counters and coefficients

    :param author_counter: Counter
        Counter where key=id, value=# of matching fos
    :param fos_counter: Counter
        Counter where key=id, value=# of matching fos
    :param keyword_counter: Counter
        Counter where key=id, value=# of matching fos
    :param author_coeff: float
        score multiplier for the number of authors
    :param fos_coeff: float
        score multiplier for the number of fos
    :param keyword_coeff: float
        score multiplier for the number of keywords
    :return score_dict: dict
        dict where key=id, value=score
    """

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


def evaluate_candidate_set_sizes(id, author_coeff, fos_coeff, keyword_coeff):
    """
    Prints the number of ground truth in the candidate set for different candidate set sizes

    :param  res: Elasticsearch result
        Elasticsearch result that contains the information for a paper
    :return keyword_list: list of Strings
        list of keyword ids
    """

    id_res = find_by_id(id)

    author_list = find_author_list(id_res)
    fos_list = find_fos_list(id_res)
    keyword_list = find_keyword_list(id_res)

    author_counter = get_author_counter(author_list)
    fos_counter = get_fos_counter(fos_list)
    keyword_counter = get_keyword_counter(keyword_list)

    score_dict = calculate_scores(author_counter, fos_counter, keyword_counter, author_coeff, fos_coeff, keyword_coeff)
    print(len(score_dict))
    id_string = str(id)
    if id_string in score_dict.keys():
        del score_dict[id_string]

    es = es_request.connect_elasticsearch()

    res = es.search(index="aminer", body={"query": {"match": {"id": id}}})

    references = res['hits']['hits'][0]['_source']['references']

    count = 0
    for r in references:
        res = es.search(index="aminer", body={"query": {"match": {"id": r}}})
        if not res['hits']['hits']:
            count += 1

    number_in_index = len(references) - count

    print("id: " + str(id))
    print("# of papers with 1 or more matching authors: " + str(len(author_counter)))
    print("# of papers with 1 or more matching fos: " + str(len(fos_counter)))
    print("# of references: " + str(len(references)))
    print("# of references in index: " + str(number_in_index))

    top100 = nlargest(100, score_dict, key=score_dict.get)
    top300 = nlargest(300, score_dict, key=score_dict.get)
    top1000 = nlargest(1000, score_dict, key=score_dict.get)
    top3000 = nlargest(3000, score_dict, key=score_dict.get)
    top10000 = nlargest(10000, score_dict, key=score_dict.get)
    top30000 = nlargest(30000, score_dict, key=score_dict.get)
    top100000 = nlargest(100000, score_dict, key=score_dict.get)

    print("candidate set size, # of references in the candidate set:")
    print("100, " + str(number_of_matches(references, top100)))
    print("300, " + str(number_of_matches(references, top300)))
    print("1000, " + str(number_of_matches(references, top1000)))
    print("3000, " + str(number_of_matches(references, top3000)))
    print("10000, " + str(number_of_matches(references, top10000)))
    print("30000, " + str(number_of_matches(references, top30000)))
    print("100000, " + str(number_of_matches(references, top100000)))


def number_of_matches(a, b):
    count = 0
    for i in a:
        if i in b:
            count += 1

    return count


def output_tuple_list_to_file(filename, list_of_tuples):
    """
    Outputs a list of tuples to a file.

    :param file: String
    :param list: list of tuples
    """

    with open(filename, 'w') as fp:
        fp.write('\n'.join('%s %s' % x for x in list_of_tuples))


# Example using id = 1558595774
# n_highest_ranking = get_highest_ranking_papers(1558595774, 1, 1, 1, 100)
# print(n_highest_ranking)


evaluate_candidate_set_sizes(1558595774, 1, 1, 1)
