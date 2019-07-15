import es_request
import logging


def find_by_id(id):
    # We should probably not be connecting each time.
    logging.basicConfig(level=logging.ERROR)
    es = es_request.connect_elasticsearch()
    res = es.search(index="aminer", body={"query": {"match" : {"id": id}}})
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


    

    
    
    