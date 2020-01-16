import json
import logging

import numpy as np

import aminer.dataset.es_request as es_request
import aminer.recall.functions as functions

folder = "../support/"


def find_by_id(id):
    """
    Takes in an id and returns information about it queried from Elasticsearch.

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


def find_by_id_cbcf_aminer(id):
    """
    Takes in an id and returns information about it queried from Elasticsearch's cbcf_aminer index.

    :param id: int
        the id of the search paper
    :return res: elasticsearch result
        Elasticsearch result that contains the information for the entered id
    """

    # We should probably not be connecting each time.
    logging.basicConfig(level=logging.ERROR)
    es = es_request.connect_elasticsearch()
    res = es.search(index="cbcf_aminer", body={"query": {"match": {"_id": id}}})
    return res


def extract_prob_vector(res):
    """
    Extracts and returns the ['_source']['vec'] field.

    :param  res: json
        Elasticsearch search result from the cbcf_aminer index
    """
    return res['hits']['hits'][0]['_source']["vec"]


def print_res(res):
    """
    Prints the '_source' field from an Elasticsearch search result

    :param  res: json
        Elasticsearch search result from the aminer index
    """

    if res['hits']['hits'] and len(res['hits']['hits']) > 0:
        print(res['hits']['hits'][0]['_source'])


def extract_references(res):
    """
    Extracts and returns the ['_source']['references'] field.

    :param  res: json
        Elasticsearch search result from the aminer index
    """

    papers = res['hits']['hits']
    reference_list = list()

    if len(papers) != 0:
        reference_list = papers[0]['_source']['references']

    return reference_list


def get_contingency_table(paper1_reference_set, paper2_reference):
    """
    Returns a 2x2 contingency table

    :param  paper1_reference_set: set
        a set of paper ids for i1
    :param  paper2_reference: list
        a list of paper ids for i2
    """
    n11 = len(paper1_reference_set.intersection(paper2_reference))

    paper1_reference_count = len(paper1_reference_set)
    paper2_reference_count = len(paper2_reference)

    n12 = paper1_reference_count - n11
    n21 = paper2_reference_count - n11
    n22 = 4107340 - paper1_reference_count - paper2_reference_count - n11

    contingency_table = [[n11, n12],
                         [n21, n22]]
    return contingency_table


def compute_score(similarity_dict, id_j):
    """
    :param  similarity_dict: dict {string, int}
        dictionary where the key is the id of the paper, and the value is the cosine similarity
    :param  id_j: string
        the id of a paper in AMiner
    :return score: int
        the score computed as defined in part III C of
        https://ieeexplore.ieee.org/document/7279056?fbclid=IwAR2YbsiF_aWB94AX_h413rlAfYqGHuBmEbusuYXSW4m1kW-eNIhxHMf1wFs
    """
    numerator = 0
    denominator = 0

    for id, similarity in similarity_dict.items():
        res = find_by_id(id)
        references = extract_references(res)

        if id_j in references:
            numerator += similarity

        denominator += similarity

    return numerator / denominator


class Memoize(object):
    def __init__(self, func):
        self.func = func
        self.eval_points = {}

    def __call__(self, *args):
        if args not in self.eval_points:
            self.eval_points[args] = self.func(*args)
        return self.eval_points[args]


def calculate_prob_vector(references_a_set, reference_list, memoized_prob):
    """
    :param references_a_set: set
        set of references for the paper belonging to this prob_vector
    :param reference_list: list of strings
        Is a list (organized by index) of lists of references (which are also paper id strings).
    :param memoized_prob: Object
        Object returned after passing functions.prob into Memoize()
    :return: prob_vector: list
        A list of prob values for each paper in the dataset with a # of references > min_references
    """
    prob_vector = [0.0] * len(reference_list)

    index = 0
    for references_b in reference_list:

        contingency_table = get_contingency_table(references_a_set, references_b)
        chi_square = functions.chi_square(contingency_table)

        rounded_chi_square = round(chi_square, 6)
        if rounded_chi_square == 0:
            continue

        prob, absolute_error = memoized_prob(rounded_chi_square)
        prob_vector[index] = prob

        # if prob >= 0.1:
        #     print(prob)

        index += 1

    return prob_vector


def extract_neighbours(pid, percent):
    """
    :param pid: int
        paper id
    :param percent:
        percent of references from pid we want for neighbour set
    :return: list, list
        first list is our neighbour set (pid we input to our algorithm)
        second list is the test set (pid we we want to predict)
        goal is to predict the second list from the first list
    """
    id_res = find_by_id(pid)
    print_res(id_res)
    reference_list = extract_references(id_res)

    neighbour_size = int(len(reference_list) * percent)
    neighbour_set = []
    if neighbour_size == len(reference_list) or neighbour_size == 0:
        print('For paper ', pid, ' the neighbour size is ', neighbour_size, 'This neighbour size cannot be used')

    for i in range(neighbour_size):
        index = np.random.randint(low=0, high=len(reference_list) - 1, size=1)[0]

        neighbour_set.append(reference_list.pop(index))

    return neighbour_set, reference_list


def populate_es_association_vectors(references_filename, inverted_index_filename):
    """
    Updates elasticsearch with the association vectors

    :param references_filename: string
        file path of a references_list json file
    :param inverted_index_filename: int
        file path of a inverted_index json file
    """
    with open(references_filename, 'r') as f:
        reference_list = json.load(f)

    with open(inverted_index_filename, 'r') as f:
        inverted_index_dict = json.load(f)

    memoized_prob = Memoize(functions.prob)

    index = 0
    for references_a in reference_list:
        prob_vector = calculate_prob_vector(set(references_a), reference_list, memoized_prob)
        id = inverted_index_dict[str(index)]

        index += 1
        es_request.upload_cbcf_vector(id, prob_vector)

        if index % 100 == 0:
            print(index)


if __name__ == '__main__':
    # neighbour_set, reference_list = extract_neighbours(2029880715, 0.5)
    # print(neighbour_set)
    # print(reference_list)

    populate_es_association_vectors("../support/references_list_40_5.json", "../support/inverted_index_dict_40_5.json")
