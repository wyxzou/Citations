import os

import pkg_resources
from aminer.precision.utility import load_json, dump_json
support_directory = pkg_resources.resource_filename("aminer", "support")
directory_2019 = os.path.join(support_directory, "2019_filters")


fos = load_json(os.path.join(directory_2019, 'fos_filtered_recommendations.json'))
author = load_json(os.path.join(directory_2019, 'author_filtered_citations.json'))
years = load_json(os.path.join(directory_2019, 'invalid_citations_based_on_year.json'))

both = {}
for id in fos:
    if id in author:
        both[id] = list(set(author[id]).intersection(set(fos[id])) - set(years))
    else:
        both[id] = list(set(fos[id]) - set(years))

dump_json(os.path.join(directory_2019, 'aggregated_filters.json'), both)
