import json
import os
import pprint

import xml.etree.ElementTree as ET

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


def extract_abstract(article_id):
    data_path = os.path.join(parentDirectory, 'dataset', 'top1000_complete')
    article_directory = os.path.join(data_path, article_id, "Documents_xml", article_id + ".xml")
    root = ET.parse(article_directory).getroot()

    abstract = []
    for child in root.find('.//ABSTRACT'):
        abstract.append(child.text)

    return ' '.join(abstract)

pp = pprint.PrettyPrinter(indent=4)
pp.pprint(extract_json('A00-1031'))

print(extract_abstract('A00-1031'))

