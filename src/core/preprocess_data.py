import json
import pandas as pd

from tqdm import tqdm

from src.core.util import dump_json, load_json, EnglishTextProcessor


class ArticleDataset():
    def __init__(self, stop_words_file, relevant_fields, input_data_path):
        self.text_processor = EnglishTextProcessor(stop_words_file)
        self.raw_data = self._extract_relevant_fields(input_data_path, relevant_fields)

    def _extract_relevant_fields(self, input_data_path, relevant_fields):
        raise NotImplementedError()


class AminerDataset(ArticleDataset):
    def __init__(self, stop_words_file, relevant_fields, articles_text_file_path):
        '''
        relevant_fields: 'id', 'title', 'year', 'indexed_abstract', 'references'
        '''
        super(AminerDataset, self).__init__(stop_words_file, relevant_fields, articles_text_file_path)

    def _extract_relevant_fields(self, articles_text_file_path, relevant_fields):
        def indexed_abstract_to_text(indexed_abstract):
            abstract_arr = ['' for _ in range(indexed_abstract['IndexLength'])]
            inverted_index = indexed_abstract['InvertedIndex']
            for key in inverted_index:
                for index in inverted_index[key]:
                    abstract_arr[index] = key

            return ' '.join(abstract_arr)

        data = []
        with open(articles_text_file_path) as articles_text_file:
            for i, line in tqdm(enumerate(articles_text_file)):
                entry = json.loads(line)
                if all([field in entry and entry[field] is not None for field in relevant_fields]):
                    data += [{
                        'id': entry['id'],
                        'title': entry['title'],
                        'year': entry['year'],
                        'abstract': self.text_processor(indexed_abstract_to_text(entry['indexed_abstract'])),
                        'references': entry['references'],
                        'fos': [self.text_processor(fos['name']) for fos in entry['fos']] if 'fos' in entry else []
                    }]

        return data


class CoreDataset(ArticleDataset):
    def __init__(self, stop_words_file, relevant_fields, articles_text_file_path):
        '''
        relevant_fields: 'id', 'title', 'year', 'description', 'fullText', 'citations'
        '''
        super(CoreDataset, self).__init__(stop_words_file, relevant_fields, articles_text_file_path)

    def _extract_relevant_fields(self, articles_json_path, relevant_fields):
        data = []
        articles = load_json(articles_json_path)
        for article in articles:
            if all([field in article and article[field] is not None for field in relevant_fields]):
                data += [{
                    'id': article['id'],
                    'title': article['title'],
                    'year': article['year'],
                    'description': self.text_processor(article['description']),
                    'full_text': self.text_processor(article['fullText']),
                    'citations': article['citations']
                }]

        return data

    def _get_raw_dataset(self):
        def get_article(articles, title, year):
            for article in articles:
                if article['title'].lower() == title.lower() and article['year'] == year:
                    return article

            return None

        entries = []
        for article in self.raw_data:
            for citation in article['citations']:
                if 'title' in citation and 'date' in citation:
                    cited_article = get_article(self.raw_data, citation['title'], citation['date'])
                    if cited_article is not None:
                        entries += [{
                            'id': article['id'],
                            'title': article['title'],
                            'year': article['year'],
                            'description': article['description'],
                            'full_text': article['full_text'],
                            'cited_article_id': cited_article['id'],
                            'cited_article_title': cited_article['title'],
                            'cited_article_year': cited_article['year'],
                            'cited_article_description': cited_article['description'],
                            'cited_article_full_text': cited_article['full_text']
                        }]

        return pd.DataFrame.from_dict(entries)

    def get_lda_dataset(self):
        raw_df = self._get_raw_dataset()
        return raw_df


def main(args):
    dataset = AminerDataset('SmartStopWords.txt', ['id', 'title', 'year', 'indexed_abstract', 'references'],
                            args.input_data_path)
    dump_json(dataset.raw_data, args.output_json_path)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    required_arguments = parser.add_argument_group('required arguments')
    required_arguments.add_argument('-a', '--input-data-path', action='store', required=True, dest='input_data_path',
                                    help='Path to input data.')
    required_arguments.add_argument('-o', '--output-json-path', action='store', required=True, dest='output_json_path',
                                    help='Path to output json file.')

    args = parser.parse_args()
    main(args)
