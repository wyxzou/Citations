import json

import aminer.dataset.create_candidate_pool as create_candidate_pool


def get_ids_from_es_object(res):
    """
    Return list of ids of paper given an Elasticsearch results object

    :param res: Elasticsearch results object
        The author id

    :return:
        Results object with list of papers
    """
    ids = []
    for doc in res['hits']['hits']:
        ids.append(doc['_source']['id'])

    return ids


def read_citation_of_papers(filename):
    """
    Reads in a file (created by find_citation_of_papers)
    and returns a dictionary of
    :param filename: string
        file to read in
    Reads in a file and returns a dict
    """

    with open(filename, "rt") as fp:
        content = json.load(fp)

    candidates = {}
    for c in content:
        for key in c:
            candidates[key] = c[key]

    return candidates


def find_citation_of_papers(input_file, output_file):
    """
    Reads a file with a list of ids, and writes the list of ids
    as well as their citations to a new file
    :param input_file: string
        name of file to read
    :param output_file: string
        name of file to
    An example of is loaded into output_data.json
    """

    content = []

    with open(input_file) as f:
        content = f.readlines()
        # you may also want to remove whitespace characters like `\n` at the end of each line
        content = [x.strip() for x in content]

    feeds = []
    ids_group = []
    for pid in content:
        candidate_dict, abstract_dict = create_candidate_pool.get_candidate_dict([pid], [], 10)
        feeds.append(candidate_dict)

    with open(output_file, mode='w', encoding='utf-8') as feedsjson:
        json.dump(feeds, feedsjson)
