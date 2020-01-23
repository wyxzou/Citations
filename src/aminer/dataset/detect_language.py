from langdetect import detect
from aminer.precision.utility import EnglishTextProcessor
from aminer.recall.query_es import get_abstract_by_pid, get_title_by_pid, get_lang_by_pid
from os import listdir
from os.path import isfile, join
import json

import pkg_resources
import logging
import aminer.dataset.es_request as es_request


def update_language(pid):
    abstract = get_abstract_by_pid(pid)
    title = get_title_by_pid(pid)

    etp = EnglishTextProcessor()

    processed_abstract = etp(abstract)
    if len(processed_abstract) > 0:
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


def update_language_all():
    logging.basicConfig(level=logging.ERROR)

    es = es_request.connect_elasticsearch()
    res = es.search(index="aminer", body={
        "_source": ["id", "abstract", "title"],
        "size": 5000,
        "query": {
            "match_all": {}
        }
    }, scroll='20m')

    # get es scroll id
    count = 0
    scroll_id = res['_scroll_id']
    while res['hits']['hits'] and len(res['hits']['hits']) > 0:
        for hit in res['hits']['hits']:
            pid = hit['_source']['id']
            abstract = hit['_source']['abstract']
            title = hit['_source']['title']

            etp = EnglishTextProcessor()
            processed_abstract = etp(abstract)

            if len(processed_abstract) > 0:
                try:
                    lang = detect(processed_abstract)
                except:
                    lang = 'Language cannot be detected'
                    print('The language of this abstract cannot be detected: ', processed_abstract)
                    print('The paper id is: ', pid)
            elif len(title) > 0:
                try:
                    lang = detect(title)
                except:
                    lang = 'Language cannot be detected'
                    print('The language of this title cannot be detected: ', title)
                    print('The paper id is: ', pid)
            else:
                lang = 'Empty abstract and title'

            dic = {
                'doc': {
                    "language": lang,
                }
            }

            response = es.update(index="aminer", id=pid, body=dic)

        count += 1
        print('Completed ', count, ' batches')
        # get es scroll ids
        scroll_id = res['_scroll_id']
        # use es scroll api
        res = es.scroll(scroll_id=scroll_id, scroll='20m', request_timeout=10)


def update_language_all_from_directory(start=0):
    support_directory = pkg_resources.resource_filename("aminer", "support")
    mypath = join(support_directory, 'abstract_dictionaries')
    files = [f for f in listdir(mypath) if isfile(join(mypath, f))]
    num_files = len(files)
    es = es_request.connect_elasticsearch()

    for i in range(start, num_files):
        filename = 'abstract_dict_' + str(i) + '.json'
        filepath = join(mypath, filename)

        with open(filepath, 'r') as json_file:
            abstract_dict = json.load(json_file)
            json_file.close()

        for pid, abstract in abstract_dict.items():
            lang = get_lang_by_pid(pid)
            if lang == 'Not inputted':
                print('Processing', pid)
                etp = EnglishTextProcessor()
                processed_abstract = etp(abstract)

                if len(abstract) > 0:
                    try:
                        lang = detect(processed_abstract)
                    except:
                        lang = 'Language cannot be detected'
                        print('The language of this abstract cannot be detected: ', processed_abstract)
                        print('The paper id is: ', pid)
                else:
                    title = get_title_by_pid(pid)
                    if len(title) > 0:
                        try:
                            lang = detect(title)
                        except:
                            lang = 'Language cannot be detected'
                            print('The language of this title cannot be detected: ', title)
                            print('The paper id is: ', pid)
                    else:
                        lang = 'Empty abstract and title'

                dic = {
                    'doc': {
                        "language": lang,
                    }
                }
                print('Language: ', lang)
                response = es.update(index="aminer", id=pid, body=dic)

        print('Completed: ', filename)


if __name__ == '__main__':
    update_language_all_from_directory(0)
