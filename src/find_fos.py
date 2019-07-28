import es_request
import logging

def find_by_id(id):
    # We should probably not be connecting each time.
    logging.basicConfig(level=logging.ERROR)
    es = es_request.connect_elasticsearch()
    res = es.search(index="aminer", body={"query": {"match": {"id": id}}})
    return res


def fos_similarity(id_1, id_2):
    id_1_res = find_by_id(id_1)
    id_2_res = find_by_id(id_2)
    id_1_fos = None
    id_2_fos = None

    if id_1_res["hits"]["total"]["value"] == 0:
        print("Did not find id 1")
        return
    elif id_1_res["hits"]["total"]["value"] == 1:
        id_1_fos = id_1_res["hits"]["hits"][0]["fos"]
    else:
        print("Found too many id 1")
        return

    if id_2_res["hits"]["total"]["value"]:
        print("Did not find id 2")
        return
    elif id_2_res["hits"]["total"]["value"]:
        id_2_fos = id_1_res["hits"]["hits"][0]["fos"]
    else:
        print("Found too many id 2")
        return

    all_fos = set()
    in_both = 0

    for fos in id_1_fos:
        all_fos.add(fos["name"])

    for fos in id_2_fos:
        if fos["name"] in all_fos:
            in_both += 1
        all_fos.add(fos["name"])

    return in_both / len(all_fos)


def extract_fos_list_from_id(id):
    res = find_by_id(id)
    papers = res['hits']['hits']
    fos_list = list()

    if len(papers) != 0:
        fos = papers[0]['_source']['fos']
        fos_list = [d['name'] for d in fos if 'name' in d]

    return fos_list


def get_papers_matching_fos(fos_list):
    id_set = set()
    logging.basicConfig(level=logging.ERROR)
    es = es_request.connect_elasticsearch()
    for fos in fos_list:
        res = es.search(index="aminer", body={
            "_source": ["id"],
            "size": 5000,
            "query": {
                "bool" : {
                    "must": [
                        { "match": {"fos.name": fos}}
                    ]
                }
            }
        }, scroll='2m')

        while res['hits']['hits'] and len(res['hits']['hits']) > 0:
            for hit in res['hits']['hits']:
                id_set.add(hit['_source']['id'])

            # get es scroll id
            scroll_id = res['_scroll_id']
            # use es scroll api
            res = es.scroll(scroll_id=scroll_id, scroll='2m',
                            request_timeout=10)
    return list(id_set)


def build_fos_dict(id_list):
    fos_dict = dict()
    for id in id_list:
        fos_list = extract_fos_list_from_id(id)
        matching_id_list = get_papers_matching_fos(fos_list)

        fos_dict[id] = matching_id_list
    return fos_dict



with open('../ids.txt') as f:
    paper_ids = f.read().splitlines()

print(build_fos_dict(paper_ids))