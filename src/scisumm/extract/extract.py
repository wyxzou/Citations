import json
import os
import pprint

parentDirectory = os.path.abspath(os.path.join(os.getcwd(), os.pardir))

def extract_json(paper_id):

    citation_list = list()

    file_path = os.path.join(parentDirectory, 'dataset', 'top1000_complete', paper_id, 'citing_sentences_annotated.json')

    with open(file_path) as json_file:
        json_data = json.load(json_file)

    for citation in json_data:
        author_list = citation.get('citing_paper_authors').split(" | ")
        citation['citing_paper_authors'] = author_list
        citation_list.append(citation)

    return citation_list

pp = pprint.PrettyPrinter(indent=4)
pp.pprint(extract_json('A00-1031'))

