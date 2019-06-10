import pandas as pd

from util import dump_json, load_json, EnglishTextProcessor

class ArticleDataset():
	def __init__(self, articles_json_path, stop_words_file):
		self.text_processor = EnglishTextProcessor(stop_words_file)

		articles = load_json(articles_json_path)
		self.raw_data = self._extract_relevant_fields(articles, ['id', 'title', 'year', 'description', 'fullText', 'citations', 'repositoryDocument'])

	def _extract_relevant_fields(self, articles, relevant_fields):
		data = []
		for article in articles[:3]:
			if all([field in article for field in relevant_fields]):
				data += [{
					'id': article['id'],
					'title': article['title'],
					'year': article['year'],
					'description': self.text_processor(article['description']),
					'full_text': self.text_processor(article['fullText']),
					'citations': article['citations'],
					'repository_document': article['repositoryDocument']
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
	dataset = ArticleDataset(args.articles_json_path, 'SmartStopWords.txt')
	df = dataset._get_raw_dataset()
	df.to_csv(args.output_csv_path)

if __name__ == '__main__':
	import argparse

	parser = argparse.ArgumentParser()
	required_arguments = parser.add_argument_group('required arguments')
	required_arguments.add_argument('-a', '--articles-json-path', action='store', required=True, dest='articles_json_path', help='Path to scraped articles json file.')
	required_arguments.add_argument('-o', '--output-csv-path', action='store', required=True, dest='output_csv_path', help='Path to output csv file.')

	args = parser.parse_args()
	main(args)