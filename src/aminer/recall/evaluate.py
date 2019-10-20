import logging
import aminer.dataset.es_request as es_request


def evaluate_recall(ground_truth, pool):
    """
    Calculates what percentage of the ground truth the pool captures

    :param ground_truth: list[int]
        A list of ids of the citations of a paper
    :param pool: list[int]
        The list of paper ids in a pool

    :return:
        The percent of ground truth pool captures
    """
    count = 0
    for i in ground_truth:
        if i in pool:
            count += 1

    return count / len(ground_truth)


def percent_references_exist(paper_id):
    """
    Given a paper id, check how many of the references are in the database

    :param paper_id: int
        Id of paper of interest

    :return:
        The percent of specified paper's citations that are in the database
    """
    logging.basicConfig(level=logging.ERROR)
    es = es_request.connect_elasticsearch()
    res = es.search(index="aminer", body={"query": {"match": {"id": paper_id}}})

    references = res['hits']['hits'][0]['_source']['references']

    if not references:
        return 1

    count = 0
    for r in references:
        res = es.search(index="aminer", body={"query": {"match": {"id": r}}})
        if not res['hits']['hits']:
            count += 1

    return (len(references) - count) / len(references)