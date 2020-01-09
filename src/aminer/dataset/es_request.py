from elasticsearch import Elasticsearch, helpers
import certifi
import logging
import json

from os import listdir
from os.path import isfile, join


class ESClient:
    def __init__(self):
        super().__init__()
        self.es = None
        self.retry_count = 5
    
    def init(self):
        for retry in range(self.retry_count):
            if self.connect_elasticsearch():
                break

    def connect_elasticsearch(self):
        _es = Elasticsearch([
            'https://search-ccf-x5pb62bjtwybf3wbaeyg2gby4y.us-east-2.es.amazonaws.com'
        ], use_ssl=True, ca_certs=certifi.where())
        if _es.ping():
            self.es = _es
            return True
        else:
            return False
    
    def search(self, index, body):
        if self.es is None:
            print("Initialize ES first")
            return None
        return self.es.search(index=index, body=body)


def connect_elasticsearch():
    _es = Elasticsearch([
        'https://search-ccf-x5pb62bjtwybf3wbaeyg2gby4y.us-east-2.es.amazonaws.com'
    ], use_ssl=True, ca_certs=certifi.where())
    if _es.ping():
        print('Yay Connect')
    else:
        print('Awww it could not connect!')
    return _es


def upload_cbcf_vector(pid, similarity):
    """

    :param pid: int
        paper id
    :param similarity: list
        vector of similarity between citing paper and all other papers
    :return:
    """
    es = connect_elasticsearch()

    dic = {
        'doc': {
            "vec": similarity,
        }
    }

    if es.exists(index="cbcf_aminer", id=pid):
        response = es.update(index="cbcf_aminer", id=pid, body=dic)
    else:
        response = es.create(index="cbcf_aminer", id=pid, body=dic)
    return response


def gendata(filename):
    """
    convert aminer json file to format that can be bulk loaded into aws elasticsearch
    :param filename: str
        json file containing aminer dataset
    :return: list[dictionary]
        data to bulk load to aws elasticsearch
    """
    json_items = []
    res = []
    with open(filename) as json_file:
        for line in json_file:
            j = json.loads(line)
            json_items.append(j)

    json_file.close()
    for json_item in json_items:
        dic = {
            "_index": "aminer",
            "_type": "document",
            "_id": json_item["id"],
            "id": json_item["id"],
            "title": json_item["title"],
            "year": json_item.get("year", -1),
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
    es = connect_elasticsearch()
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
    """
    :param inverted_index:
        dictionary of inverted indices
    :return: str
        abstract as string
    """
    abstract = ['']*inverted_index['IndexLength']
    for key, val in inverted_index['InvertedIndex'].items():
        for v in val:
            abstract[v] = key

    abstract_str = ' '.join([str(elem) for elem in abstract])

    return abstract_str


def load_data_incrementally():
    """
    Function to load json files and upload them to aws.
    Due to file size being too large, we split the original file into many smaller files and upload them individually.
    Repeated uploads necessary since connection failure is common.
    Note: library bigjson does not work with our file format.
    """
    mypath = 'C:\\Users\\William\\Documents\\Citations\\src\\aminer\\data\\split'
    # files = [f for f in listdir(mypath) if isfile(join(mypath, f))]
    # ['xaa', 'xab', 'xac']
    # ['xad', 'xae', 'xaf']
    # ['xag', 'xah', 'xai']
    # ['xaj', 'xak', 'xal']
    # ['xam', 'xan']
    # ['xao', 'xap', 'xaq', 'xar', 'xas']
    # ['xat', 'xau', 'xav']
    # ['xaw', 'xax', 'xay', 'xaz']
    # ['xba', 'xbb', 'xbc', 'xbd']
    # ['xbe', 'xbf', 'xbg']
    # ['xbh', 'xbi', 'xbj']
    # ['xbk', 'xbl', 'xbm', 'xbn']
    files = ['xbo', 'xbp']

    for file in files:
        path = join(mypath, file)
        print(path)
        data = gendata(path)
        es = connect_elasticsearch()
        helpers.bulk(es, data)


if __name__ == '__main__':
    logging.basicConfig(level=logging.ERROR)
    # es = connect_elasticsearch()

    # load_data_incrementally()
    # Uncomment the below two lines to generate and populate the index
    # data = gendata()

    # helpers.bulk(es, data)

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

    upload_cbcf_vector(2029880718, [1, 2, 3])
