from util import dump_json, load_json, EnglishTextProcessor

class ArticleDataset():
	def __init__(self, articles_json_path):
		self.text_processor = EnglishTextProcessor('SmartStopWords.txt')

		articles = load_json(articles_json_path)
		self.raw_data = self._get_raw_dataset(articles)

	def _get_raw_dataset(self, articles):
		data = []
		for article in articles:
			required_fields = ['id', 'title', 'year', 'description', 'fullText', 'citations', 'repositoryDocument']
			if all([field in article for field in required_fields]):
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
	
	def get_lda_dataset(self):
		pass

def main(args):
	dataset = ArticleDataset(args.articles_json_path)
	dump_json(dataset.raw_data, args.output_json_path)

if __name__ == '__main__':
	import argparse

	parser = argparse.ArgumentParser()
	required_arguments = parser.add_argument_group('required arguments')
	required_arguments.add_argument('-a', '--articles-json-path', action='store', required=True, dest='articles_json_path', help='Path to scraped articles json file.')
	required_arguments.add_argument('-o', '--output-json-path', action='store', required=True, dest='output_json_path', help='Path to output json file.')

	args = parser.parse_args()
	main(args)