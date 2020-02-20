import os

from tqdm import tqdm
import pkg_resources

from aminer.precision.utility import load_json, dump_json

support_directory = pkg_resources.resource_filename("aminer", "support")


def find_invalid_years(years):
    papers_with_invalid_years = []
    for i in tqdm(range(411)):
        a = load_json(os.path.join(support_directory, 'year_dictionaries/year_dict_{}.json'.format(i)))
        for pid, pyear in a.items():
            if pyear in years:
                papers_with_invalid_years.append(pid)

    dump_json(os.path.join(support_directory, 'invalid_citations_based_on_year.json'), invalid_years)


if __name__ == '__main__':
    find_invalid_years([2018, 2019])
