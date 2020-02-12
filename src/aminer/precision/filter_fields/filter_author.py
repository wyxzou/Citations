import os
import json

from tqdm import tqdm
import pkg_resources

from aminer.precision.utility import load_json, dump_json
support_directory = pkg_resources.resource_filename("aminer", "support")
file = os.path.join('../support', '2019_ids.txt')
ids = set([line.rstrip('\n') for line in open(file)])


paper_authors = {}
for i in tqdm(range(411)):
    a = load_json(os.path.join(support_directory, 'author_dictionaries/author_dict_{}.json'.format(i)))
    for id in a:
        paper_authors[id] = a[id]


first_order_author_dicts = {}
for i in tqdm(range(411)):
    c = load_json(os.path.join(support_directory, 'cited_author_dictionaries/cited_author_dict_{}.json'.format(i)))
    for id in c:
        if id in ids:
            continue

        authors = paper_authors[id]
        cited_authors = c[id]
        for author in authors:
            if author not in first_order_author_dicts:
                first_order_author_dicts[author] = set()

            first_order_author_dicts[author] = first_order_author_dicts[author].union(cited_authors)

found = 0
found_ids = []
for id in ids:
    if not any(author in first_order_author_dicts for author in paper_authors[id]):
        print(id)
    else:
        found_ids += [id]

ids = found_ids

potentially_cited_authors_o1 = {id: set() for id in ids}
potentially_cited_authors_o2 = {id: set() for id in ids}
for id in tqdm(ids):
    for author in paper_authors[id]:
        if author in first_order_author_dicts:
            potentially_cited_authors_o2[id] = potentially_cited_authors_o2[id].union(first_order_author_dicts[author])
            potentially_cited_authors_o1[id] = potentially_cited_authors_o1[id].union(first_order_author_dicts[author])

            for c_author in first_order_author_dicts[author]:
                if c_author in first_order_author_dicts:
                    potentially_cited_authors_o2[id] = potentially_cited_authors_o2[id].union(
                        first_order_author_dicts[c_author])

    print(len(potentially_cited_authors_o1[id]), len(potentially_cited_authors_o2[id]))

dump_json(os.path.join(support_directory, 'p_o1.json'), {i:list(potentially_cited_authors_o1[i]) for i in potentially_cited_authors_o1})
dump_json(os.path.join(support_directory, 'p_o2.json'), {i:list(potentially_cited_authors_o2[i]) for i in potentially_cited_authors_o2})

ids = [i for i in ids if i not in ['1992078202', '2004111797', '1551676982', '2163666621', '1575030031']]
potentially_cited_authors_o1 = load_json(os.path.join(support_directory, 'p_o1.json'))
potentially_cited_authors_o2 = load_json(os.path.join(support_directory, 'p_o2.json'))

sum(len(potentially_cited_authors_o1[i]) for i in ids) / len(ids), sum(len(potentially_cited_authors_o2[i]) for i in ids) / len(ids)

author_to_paper = {}
for paper in tqdm(paper_authors):
    for author in paper_authors[paper]:
        if author not in author_to_paper:
            author_to_paper[author] = []

        author_to_paper[author] += [paper]


potentially_cited_authors_for_id = potentially_cited_authors_o1
potential_citations = {id: set() for id in ids}
for id in tqdm(potentially_cited_authors_for_id):
    for author in potentially_cited_authors_for_id[id]:
        if author in author_to_paper:
            potential_citations[id] = potential_citations[id].union(author_to_paper[author])


sum(len(potential_citations[id]) for id in ids) / len(potential_citations)
sum(len(potential_citations[id]) for id in ids) / len(potential_citations)


potentially_cited_authors_for_id = {id: set() for id in ids}
for id in ids:
    for author in paper_authors[id]:
        if author in first_order_author_dicts:
            potentially_cited_authors_for_id[id] = potentially_cited_authors_for_id[id].union(first_order_author_dicts[author])


from aminer.recall.query_es import get_abstract_by_pids, get_references_by_pid

test_recs = {}
for id in ids:
    test_recs[id] = get_references_by_pid(id)

overall = 0
for id in tqdm(ids):
    points = 0
    for rec_id in test_recs[id]:
        points += int(rec_id in potential_citations[id])

    overall += points / len(test_recs[id])
    print(points / len(test_recs[id]))


dump_json(os.path.join(support_directory, 'author_filtered_citations.json'), {id:list(potential_citations[id]) for id in potential_citations})

