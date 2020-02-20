import os

from tqdm import tqdm
import pkg_resources

from aminer.precision.utility import load_json, dump_json

support_directory = pkg_resources.resource_filename("aminer", "support")


def find_invalid_lang():
    papers_with_invalid_languages = []
    for i in tqdm(range(411)):
        a = load_json(os.path.join(support_directory, 'language_dictionaries/language_dict_{}.json'.format(i)))
        for pid, plang in a.items():
            if plang != 'en':
                papers_with_invalid_languages.append(pid)

    dump_json(os.path.join(support_directory, 'invalid_citations_based_on_year.json'), papers_with_invalid_languages)


if __name__ == '__main__':
    find_invalid_lang()
