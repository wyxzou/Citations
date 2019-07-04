import es_request
import logging


def find_author(author_id, author_name):
    logging.basicConfig(level=logging.ERROR)
    es = es_request.connect_elasticsearch()    

    res = es.search(index="aminer", body={
        "query": {
            "bool" : {
                "must": [
                    { "match": {"authors.name": author_name} },
                    { "match": {"authors.id": author_id} }
                ]
            }
        }
    })

    return res


if __name__ == '__main__':
    print(find_author('2702511795', 'Peter Kostelnik'))