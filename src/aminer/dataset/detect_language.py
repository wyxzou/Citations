from langdetect import detect
from aminer.precision.utility import EnglishTextProcessor
from aminer.recall.query_es import get_abstract_by_pid, get_title_by_pid

import logging
import aminer.dataset.es_request as es_request


def update_language(pid):
    abstract = get_abstract_by_pid(pid)
    title = get_title_by_pid(pid)

    etp = EnglishTextProcessor()

    if len(abstract) > 0:
        processed_abstract = etp(abstract)
        lang = detect(processed_abstract)
    elif len(title) > 0:
        lang = detect(title)
    else:
        lang = 'Cannot be detected'

    dic = {
        'doc': {
            "language": lang,
        }
    }

    es = es_request.connect_elasticsearch()
    response = es.update(index="aminer", id=pid, body=dic)

    return response


if __name__ == '__main__':
    logging.basicConfig(level=logging.ERROR)
    etp = EnglishTextProcessor()

    es = es_request.connect_elasticsearch()
    res = es.search(index="aminer", body={
        "_source": ["id", "abstract", "title"],
        "size": 10000,
        "query": {
            "match_all": {}
        }
    }, scroll='2m')

    # get es scroll id
    scroll_id = res['_scroll_id']
    while res['hits']['hits'] and len(res['hits']['hits']) > 0:
        for hit in res['hits']['hits']:
            pid = hit['_source']['id']
            abstract = hit['_source']['abstract']
            title = hit['_source']['title']

            etp = EnglishTextProcessor()

            if len(abstract) > 0:
                processed_abstract = etp(abstract)
                lang = detect(processed_abstract)
            elif len(title) > 0:
                lang = detect(title)
            else:
                lang = 'Cannot be detected'

            dic = {
                'doc': {
                    "language": lang,
                }
            }

            es = es_request.connect_elasticsearch()
            response = es.update(index="aminer", id=pid, body=dic)

        # get es scroll id
        scroll_id = res['_scroll_id']
        # use es scroll api
        res = es.scroll(scroll_id=scroll_id, scroll='2m', request_timeout=10)

