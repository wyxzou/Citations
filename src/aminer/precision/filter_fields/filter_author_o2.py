import os

from tqdm import tqdm
import pkg_resources

from aminer.precision.utility import load_json, dump_json
from aminer.recall.query_es import get_references_by_pid

support_directory = pkg_resources.resource_filename("aminer", "support")
file = os.path.join(support_directory, '2019_ids.txt')
ids = set([line.rstrip('\n') for line in open(file)])


paper_authors = load_json(os.path.join(support_directory, 'temp_paper_authors.json'))
first_order_author_dicts = load_json(os.path.join(support_directory, 'temp_first_order_author_dicts.json'))

ids = [i for i in ids if i not in ['1992078202', '2004111797', '1551676982', '2163666621', '1575030031']]
potentially_cited_authors_o1 = load_json(os.path.join(support_directory, 'p_o1.json'))
potentially_cited_authors_o2 = load_json(os.path.join(support_directory, 'p_o2.json'))

id_intersect = set(potentially_cited_authors_o1.keys()).intersection(ids)

sum(len(potentially_cited_authors_o1[i]) for i in id_intersect) / len(ids), sum(len(potentially_cited_authors_o2[i]) for i in id_intersect) / len(ids)


author_to_paper = {}
for paper in tqdm(paper_authors):
    for author in paper_authors[paper]:
        if author not in author_to_paper:
            author_to_paper[author] = []

        author_to_paper[author] += [paper]


potentially_cited_authors_for_id = potentially_cited_authors_o2
potential_citations = {id: set() for id in ids}
for id in tqdm(potentially_cited_authors_for_id):
    print(id)
    print(len(potentially_cited_authors_for_id[id]))

    for author in potentially_cited_authors_for_id[id]:

        if author in author_to_paper:
            potential_citations[id].update(author_to_paper[author])

dump_json(os.path.join(support_directory, 'author_filtered_citations_o2.json'), {id:list(potential_citations[id]) for id in potential_citations})