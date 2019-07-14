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
    with open('data/out.json') as json_file:  
        counter = 0
        for line in json_file:
            counter += 1
            if counter > 1000:
                break
            j = json.loads(line)
            json_items.append(j)

    # print(json_items[0])
    for json_item in json_items:
        # print(json_items)
        # print(json_item["indexed_abstract"])
        res.append({
            "_index": "aminer",
            "_type": "document",
            "id": json_item["id"],
            "title": json_item["title"],
            "year" : json_item["year"],
            "references": json_item.get("references", []),
            "authors": json_item.get("authors", []),
            "keywords": json_item.get("keywords", []),
            "fos" : json_item.get("fos", []),
            # "indexed_abstract" : json_item.get("indexed_abstract", {})    
        })

    return res

if __name__ == '__main__':
    logging.basicConfig(level=logging.ERROR)
    es = connect_elasticsearch()
    # Uncomment the below two lines to generate and populate the index
    data = gendata()
    helpers.bulk(es, data)

    # To search for lines in aminer index with empty query
    # res = es.search(index="aminer", body="")
    # print("Found ", res["hits"]["total"]["value"])
    # for i in res["hits"]["hits"]:
    #    print(i)

    # To search for lines in aminer index with specific id
    # res = es.search(index="aminer", body={"query": {"match" : {"id": "100008749"}}})
    # print("Found ", res["hits"]["total"]["value"])
    # for i in res["hits"]["hits"]:
    #     print(i)
    
    
