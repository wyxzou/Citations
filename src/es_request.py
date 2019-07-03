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
    res = []
    with open('../dblp_papers_v11.txt') as json_file:  
        counter = 0
        for line in json_file:
            counter += 1
            if counter > 100:
                break
            j = json.loads(line)
            json_items.append(j)
    for json_item in json_items:
        res.append({
            "_index": "aminer",
            "_type": "document",
            "doc": {
                "id": json_item["id"],
                "title": json_item["title"],
                "references": json_item.get("references", [])
            },
        })

    return res

if __name__ == '__main__':
    logging.basicConfig(level=logging.ERROR)
    es = connect_elasticsearch()
    data = gendata()
    helpers.bulk(es, data)