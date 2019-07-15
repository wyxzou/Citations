from elasticsearch import Elasticsearch
import logging

def connect_elasticsearch():
    _es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    if _es.ping():
        print('Yay Connect')
    else:
        print('Awww it could not connect!')
    return _es

def search(es, query={}, size=10000):
    res = es.search(index="aminer", size=size, body=query)
    return res["hits"]["hits"]

if __name__ == '__main__':
    logging.basicConfig(level=logging.ERROR)
    es = connect_elasticsearch()
    query = {}
    # returns up to 10000 papers that match the query
    papers = search(es, query=query)

    # We want to do some analytics to determine top 1000