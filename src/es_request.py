from elasticsearch import Elasticsearch, helpers
import logging
import json

def connect_elasticsearch():
    _es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    if _es.ping():
        print('Yay Connect')
    else:
        print('Awww it could not connect!')
    return _es

def gendata():
    json_items = []
    with open('../repository_metadata_2013-12-15/repository_metadata_2013-12-09_2.json') as json_file:  
        for line in json_file:
            j = json.loads(line)
            json_items.append(j)
    for json_item in json_items:
        print(json_item["identifier"])
        yield {
            "_index": "id",
            "_type": "document",
            "doc": {"word": json_item["identifier"]},
        }

if __name__ == '__main__':
  logging.basicConfig(level=logging.ERROR)
  es = connect_elasticsearch()
  helpers.bulk(es, gendata())