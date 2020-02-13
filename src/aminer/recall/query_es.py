import logging
import random

import aminer.dataset.es_request as es_request


def get_authors_by_pid(paper_id):
    """
    Gets the author_ids associated with a paper_idd

    :param paper_ids: int
        The id of the paper we want to find the authors of
    :return:
        Dictionary with paper id as key and abstract as value
    """
    logging.basicConfig(level=logging.ERROR)
    es = es_request.connect_elasticsearch()

    res = es.search(index="aminer", body={"query": {"match": {"_id": paper_id}}})

    author_list = res['hits']['hits'][0]['_source']['authors']

    author_ids = []
    for author in author_list:
        author_ids.append(author['id'])

    return author_ids


def get_es_object_by_pid(paper_id):
    # We should probably not be connecting each time.
    logging.basicConfig(level=logging.ERROR)
    es = es_request.connect_elasticsearch()
    res = es.search(index="aminer", body={"query": {"match": {"_id": paper_id}}})
    return res


def get_year_by_pid(paper_id):
    logging.basicConfig(level=logging.ERROR)
    es = es_request.connect_elasticsearch()
    res = es.search(index="aminer", body={"query": {"match": {"_id": paper_id}}})

    if len(res['hits']['hits']) == 0:
        return ''

    abstract = res['hits']['hits'][0]['_source']['year']
    return abstract


def get_abstract_by_pid(paper_id):
    logging.basicConfig(level=logging.ERROR)
    es = es_request.connect_elasticsearch()
    res = es.search(index="aminer", body={"query": {"match": {"_id": paper_id}}})

    if len(res['hits']['hits']) == 0:
        return ''

    abstract = res['hits']['hits'][0]['_source']['abstract']
    return abstract


def get_abstract_by_pids(paper_ids):
    """
    Gets the abstract of papers with the provided list of ids

    :param paperids: list[int]
        The list of paper ids to find the abstract for
    :return:
        Dictionary with paper id as key and abstract as value
    """
    es = es_request.connect_elasticsearch()

    dic = {}
    for paper_id in paper_ids:
        res = es.search(index="aminer", body={"query": {"match": {"_id": paper_id}}})
        if not res["hits"]["hits"]:
            print("Error: no such id for ", paper_id)
        else:
            dic[paper_id] = res["hits"]["hits"][0]['_source']['abstract']

    return dic


def get_references_by_pid(paper_id):
    logging.basicConfig(level=logging.ERROR)
    es = es_request.connect_elasticsearch()
    res = es.search(index="aminer", body={"query": {"match": {"_id": paper_id}}})

    if len(res['hits']['hits']) == 0:
        return list()

    reference_list = res['hits']['hits'][0]['_source']['references']
    return reference_list


def get_lang_and_fos_by_pid(paper_id):
    logging.basicConfig(level=logging.ERROR)
    es = es_request.connect_elasticsearch()
    res = es.search(index="aminer", body={"query": {"match": {"_id": paper_id}}})

    if len(res['hits']['hits']) == 0:
        return list()

    fields = res['hits']['hits'][0]['_source']
    if 'language' in fields:
        lang = res['hits']['hits'][0]['_source']['language']
    else:
        lang = 'Not inputted'

    fos = res['hits']['hits'][0]['_source']['fos']

    return lang, fos

def get_lang_by_pid(paper_id):
    logging.basicConfig(level=logging.ERROR)
    es = es_request.connect_elasticsearch()
    res = es.search(index="aminer", body={"query": {"match": {"_id": paper_id}}})

    if len(res['hits']['hits']) == 0:
        return list()

    fields = res['hits']['hits'][0]['_source']
    if 'language' in fields:
        lang = res['hits']['hits'][0]['_source']['language']
    else:
        lang = 'Not inputted'

    return lang


def get_fos_by_pid(paper_id):
    logging.basicConfig(level=logging.ERROR)
    es = es_request.connect_elasticsearch()
    res = es.search(index="aminer", body={"query": {"match": {"_id": paper_id}}})

    if len(res['hits']['hits']) == 0:
        return list()

    fos = res['hits']['hits'][0]['_source']['fos']
    return fos


def get_title_by_pid(paper_id):
    logging.basicConfig(level=logging.ERROR)
    es = es_request.connect_elasticsearch()
    res = es.search(index="aminer", body={"query": {"match": {"_id": paper_id}}})

    if len(res['hits']['hits']) == 0:
        return list()

    title = res['hits']['hits'][0]['_source']['title']
    return title


def get_papers_with_author_id(author_id):
    """
    Return all results with a specific author id. Returns an elasticsearch object.
    Use with get_ids_from_es_object in utility to return papers

    :param author_id: int
        The author id

    :return:
        Elasticserach results object associated with provided author id
    """
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