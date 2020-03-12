import os

import pkg_resources
from aminer.precision.utility import load_json, dump_json

support_directory = pkg_resources.resource_filename("aminer", "support")
directory_2019 = os.path.join(support_directory, "2019_filters")
directory_keywords = os.path.join(support_directory, "keyword_filtering")

fos = load_json(os.path.join(directory_2019, 'fos_filtered_recommendations.json'))
author = load_json(os.path.join(directory_2019, 'author_filtered_citations.json'))
years = load_json(os.path.join(directory_2019, 'invalid_citations_based_on_year.json'))
language = load_json(os.path.join(directory_2019, 'invalid_citations_based_on_language.json'))
keywords = load_json(os.path.join(directory_keywords, 'keywords_top_10000.json'))

both = {}
for id in fos:
    if id in author and id in keywords:
        both[id] = list(set(author[id]).intersection(set(fos[id])).intersection(set(keywords[id])) - set(years) - set(language))
    elif id in keywords and id not in author:
        both[id] = list(set(fos[id]).intersection(set(keywords[id])) - set(years) - set(language))
    elif id in author and id not in keywords:
        both[id] = list(set(fos[id]).intersection(set(author[id])) - set(years) - set(language))
    else:
        both[id] = list(set(fos[id]) - set(years) - set(languages))

dump_json(os.path.join(directory_2019, 'aggregated_filters.json'), both)
