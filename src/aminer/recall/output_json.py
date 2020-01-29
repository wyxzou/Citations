import json
import logging

import aminer.dataset.es_request as es_request

folder = "../support/"


def output_index_dict(min_references, min_cited_by):
    """
    :param  min_references: int
        filters out all papers with a number of references less than min_references
    :param  min_cited_by: int
        filters out all papers that are cited by fewer than min_cited_by papers
    :return index_dict: dict {string, int}
        dictionary where key is paper id, and value is the vector index.
    """

    cited_by_dict = dict()
    if min_cited_by != 0:
        with open(folder + "cited_by.json", 'r') as f:
            cited_by_dict = json.load(f)

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

            if min_cited_by == 0 or (
                    len(cited_by_dict) == 0 and id in cited_by_dict and cited_by_dict[id] >= min_cited_by):
                references = hit['_source']['references']
                if len(references) >= min_references:
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

    with open(folder + "index_dict_" + str(min_references) + '_' + str(min_cited_by) + ".json", 'w') as fp:
        fp.write(json.dumps(index_dict))

    print(len(index_dict))
    return index_dict


def output_cited_by_dict():
    """
        Outputs dict to JSON file which maps each id to the number of times that id has been cited by other papers.
    """

    logging.basicConfig(level=logging.ERROR)
    es = es_request.connect_elasticsearch()

    res = es.search(index="aminer", body={
        "_source": ["id", "references"],
        "size": 10000,
        "query": {
            "match_all": {}
        }
    }, scroll='2m')

    id_set = set()
    cited_by_dict = dict()

    # get es scroll id
    scroll_id = res['_scroll_id']
    while res['hits']['hits'] and len(res['hits']['hits']) > 0:

        for hit in res['hits']['hits']:
            id = hit['_source']['id']
            references = hit['_source']['references']

            if id not in id_set:
                cited_by_dict = increment_dict(cited_by_dict, references)
                id_set.add(id)

        print(len(id_set))

        # use es scroll api
        res = es.scroll(scroll_id=scroll_id, scroll='2m',
                        request_timeout=10)

    es.clear_scroll(body={'scroll_id': scroll_id})

    with open(folder + "cited_by.json", 'w') as fp:
        json.dump(cited_by_dict, fp)


def increment_dict(cited_by_dict, reference_list):
    """
        Increments items in the cited_by_dict using the elements of the reference_list parameter.
    """
    reference_set = set(reference_list)
    for reference in reference_set:
        if reference in cited_by_dict:
            cited_by_dict[reference] = cited_by_dict[reference] + 1
        else:
            cited_by_dict[reference] = 1
    return cited_by_dict


def output_reference_list(min_references, min_cited_by):
    """
    :param  min_references: int
        filters out all papers with a number of references less than min_references
    :param  min_cited_by: int
        filters out all papers that are cited by fewer than min_cited_by papers
    :return reference_list: list of strings
        Is a list (organized by index) of lists of references (which are also paper id strings).
    """

    logging.basicConfig(level=logging.ERROR)
    es = es_request.connect_elasticsearch()

    with open(folder + "index_dict_" + str(min_references) + '_' + str(min_cited_by) + ".json", 'r') as f:
        index_dict = json.load(f)

    reference_list = [0.0] * len(index_dict)

    res = es.search(index="aminer", body={
        "_source": ["id", "references"],
        "size": 10000,
        "query": {
            "match_all": {}
        }
    }, scroll='2m')

    # get es scroll id
    scroll_id = res['_scroll_id']

    i = 0
    while res['hits']['hits'] and len(res['hits']['hits']) > 0:
        for hit in res['hits']['hits']:
            id = hit['_source']['id']

            if id in index_dict and id not in reference_list:
                references = hit['_source']['references']
                references_as_ints = list(map(int, references))

                list_index = index_dict[id]

                reference_list[list_index] = references_as_ints
                i += 1
                if i % 100 == 0:
                    print(i)

        # use es scroll api
        res = es.scroll(scroll_id=scroll_id, scroll='2m',
                        request_timeout=10)

    print(i)
    es.clear_scroll(body={'scroll_id': scroll_id})

    with open(folder + "references_list_" + str(min_references) + '_' + str(min_cited_by) + ".json", 'w') as fp:
        json.dump(reference_list, fp)
    return reference_list


def output_reference_dict(min_references, min_cited_by):
    """
    :param  min_references: int
        filters out all papers with a number of references less than min_references
    :param  min_cited_by: int
        filters out all papers that are cited by fewer than min_cited_by papers
    :return reference_dict: dict {string: list of strings}
        Is a dict mapping an id to that id's lists of references (which are also paper id strings).
    """

    logging.basicConfig(level=logging.ERROR)
    es = es_request.connect_elasticsearch()

    cited_by_dict = dict()
    if min_cited_by != 0:
        with open(folder + "cited_by.json", 'r') as f:
            cited_by_dict = json.load(f)

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

            if min_cited_by == 0 or (
                    len(cited_by_dict) == 0 and id in cited_by_dict and cited_by_dict[id] >= min_cited_by):
                references = hit['_source']['references']
                if len(references) >= min_references:
                    references_as_ints = list(map(int, references))
                    reference_dict[id] = references_as_ints

        print(len(reference_dict))
        # use es scroll api
        res = es.scroll(scroll_id=scroll_id, scroll='2m',
                        request_timeout=10)

    es.clear_scroll(body={'scroll_id': scroll_id})

    with open(folder + "reference_dict_" + str(min_references) + '_' + str(min_cited_by) + ".json", 'w') as fp:
        json.dump(reference_dict, fp)
    return reference_dict


def output_inverted_index_dict(min_references, min_cited_by):
    """
    :param  min_references: int
        filters out all papers with a number of references less than min_references
    :param  min_cited_by: int
        filters out all papers that are cited by fewer than min_cited_by papers
    :return: inverted_dict: dict
        dictionary with the keys and values of the index dict swapped
    """

    with open(folder + "index_dict_" + str(min_references) + '_' + str(min_cited_by) + ".json", 'r') as f:
        index_dict = json.load(f)

    inverted_dict = dict([[v, k] for k, v in index_dict.items()])

    with open(folder + "inverted_index_dict_" + str(min_references) + '_' + str(min_cited_by) + ".json", 'w') as fp:
        fp.write(json.dumps(inverted_dict))
    return inverted_dict


def output_title_dict():
    """
        For each batch the function creates a JSON file of a dict mapping each id to that id's title
    """

    logging.basicConfig(level=logging.ERROR)
    es = es_request.connect_elasticsearch()

    res = es.search(index="aminer", body={
        "_source": ["id", "title"],
        "size": 10000,
        "query": {
            "match_all": {}
        }
    }, scroll='2m')

    id_set = set()

    i = 0
    # get es scroll id
    scroll_id = res['_scroll_id']
    while res['hits']['hits'] and len(res['hits']['hits']) > 0:

        title_dict = dict()

        for hit in res['hits']['hits']:
            id = hit['_source']['id']

            if id not in id_set:
                title_dict[id] = hit['_source']['title']
                id_set.add(id)

        print(len(id_set))

        with open("../support/title/title_dict_" + str(i) + ".json", 'w') as fp:
            json.dump(title_dict, fp)

        # use es scroll api
        res = es.scroll(scroll_id=scroll_id, scroll='2m',
                        request_timeout=10)
        i += 1
    es.clear_scroll(body={'scroll_id': scroll_id})


def output_abstract_dict():
    """
        For each batch the function creates a JSON file of a dict mapping each id to that id's abstract
    """

    logging.basicConfig(level=logging.ERROR)
    es = es_request.connect_elasticsearch()

    res = es.search(index="aminer", body={
        "_source": ["id", "abstract", "language", "fos"],
        "size": 10000,
        "query": {
            "match_all": {}
        }
    }, scroll='2m')

    id_set = set()

    i = 0
    # get es scroll id
    scroll_id = res['_scroll_id']
    while res['hits']['hits'] and len(res['hits']['hits']) > 0:

        abstract_dict = dict()

        for hit in res['hits']['hits']:
            id = hit['_source']['id']

            if id not in id_set:
                abstract = hit['_source']['abstract']

                abstract_dict[id] = abstract
                id_set.add(id)

        print(len(id_set))

        with open("../support/abstract/abstract_dict_" + str(i) + ".json", 'w') as fp:
            json.dump(abstract_dict, fp)

        # use es scroll api
        res = es.scroll(scroll_id=scroll_id, scroll='2m',
                        request_timeout=10)
        i += 1
    es.clear_scroll(body={'scroll_id': scroll_id})


if __name__ == '__main__':
# output_cited_by_dict()
# output_index_dict(40, 5)
# output_reference_list(40, 5)
# output_inverted_index_dict(40, 5)
