from elasticsearch import Elasticsearch, helpers
import logging
import json


def connect_elasticsearch():
    # _es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    # _es = Elasticsearch([{
    #     'host': 'https://search-ccf-x5pb62bjtwybf3wbaeyg2gby4y.us-east-2.es.amazonaws.com',
    #     'port': 80
    # }])
    _es = Elasticsearch([
        'https://search-ccf-x5pb62bjtwybf3wbaeyg2gby4y.us-east-2.es.amazonaws.com'
    ])
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
            if counter > 2000:
                break
            j = json.loads(line)
            json_items.append(j)

    for json_item in json_items:
        print(json_item["title"])
        dic = {
            "_index": "aminer",
            "_type": "document",
            "_id": json_item["id"],
            "id": json_item["id"],
            "title": json_item["title"],
            "year": json_item["year"],
            "references": json_item.get("references", []),
            "authors": json_item.get("authors", []),
            "keywords": json_item.get("keywords", []),
            "fos": json_item.get("fos", []),
        }

        if "indexed_abstract" in json_item:
            k = convert_inverted_index(json_item["indexed_abstract"])
            dic["abstract"] = k
        else:
            dic["abstract"] = ''
        
        res.append(dic)

    return res


def create_index(index):
    """
    Attempt at creating a mapping for aminer index
    :param index: the index in elasticsearch database
    """
    request_body = {
        'mappings': {
            'regular': {
                'properties': {
                    "id": {'type': 'integer'},
                    'title': {'type': 'string'},
                    'year': {'type': 'integer'},
                    'references': {'type': 'object'},
                    'authors': {'type': 'object'},
                    'keywords': {'type': 'object'},
                    'fos': {'type': 'object'},
                    'abstract': {'type': 'object',
                                 "properties": {
                                    "key": {"type": "string", "index": "not_analyzed"},
                                    "value": {"type": "object", "index": "not_analyzed"}
                                }
                    }
                }
            }
        }
    }
    print("creating 'example_index' index...")
    es.indices.create(index=index, body=request_body)


def convert_inverted_index(inverted_index):
    abstract = ['']*inverted_index['IndexLength']
    for key, val in inverted_index['InvertedIndex'].items():
        for v in val:
            abstract[v] = key

    abstract_str = ' '.join([str(elem) for elem in abstract])
    return abstract_str


if __name__ == '__main__':
    logging.basicConfig(level=logging.ERROR)
    es = connect_elasticsearch()
    # Uncomment the below two lines to generate and populate the index
    data = gendata()

    helpers.bulk(es, data)

    # Insert single data in dataset. data is generated from gendata()
    # for d in data:
    #     id = d.pop("_id")
    #     doc_type = d.pop("_type")
    #     index = d.pop("_index")
    #
    #     es.index(index=index, doc_type=doc_type, id=id, body=d)

    # To search for lines in aminer index with empty query
    # res = es.search(index="aminer", body="")
    # print("Found ", res["hits"]["total"]["value"])
    # for i in res["hits"]["hits"]:
    #    print(i)
    #
    # To search for lines in aminer index with specific id
    # res = es.search(index="aminer", body={"query": {"match" : {"id": "100008749"}}})
    # print("Found ", res["hits"]["total"]["value"])
    # for i in res["hits"]["hits"]:
    #     print(i)
