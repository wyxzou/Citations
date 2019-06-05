import json
import requests
import sys
import util

from time import sleep

class CoreAPIDataCollector():
	def __init__(self, core_api_access_token=None):
		import requests

		from urllib3.util.retry import Retry
		from requests.adapters import HTTPAdapter

		self.core_api_access_token = core_api_access_token

		request_retries = Retry(total=20, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
		self.request_session = requests.Session()
		self.request_session.mount('http://', HTTPAdapter(max_retries=request_retries))

	def _create_query_for_topics(self, required_topics, optional_topics=None):
		def create_query_for_topic(topic):
			keywords = ' AND '.join(topic.split())
			return '(title:({0}) OR description:({0}) OR fullText:({0}))'.format(keywords)

		required_topic_queries = [create_query_for_topic(topic) for topic in required_topics]
		if optional_topics is not None:
			optional_topic_queries = [create_query_for_topic(topic) for topic in optional_topics]
			optional_query = '({})'.format(' OR '.join(optional_topic_queries))
			required_topic_queries += [optional_query]

		query = '({})'.format(' AND '.join(required_topic_queries))
		return query

	def get_articles_for_topics(self, required_topics, optional_topics=None):
		import urllib.parse

		query = self._create_query_for_topics(required_topics, optional_topics)
		request_url = 'https://core.ac.uk:443/api-v2/articles/search/{}'.format(urllib.parse.quote(query))
		params = {
			'pageSize': 100,
			'metadata': True,
			'fulltext': True,
			'citations': True,
			'similar': True,
			'duplicate': False,
			'urls': True,
			'faithfulMetadata': False,
			'apiKey': self.core_api_access_token
		}
		articles = self.get_api_objects(request_url, params=params)
		return articles

	def get_api_objects(self, object_url, params=None, max_pages=10000):
		objects = []
		for i in range(1, max_pages+1):
			response = requests.get('{}?page={}'.format(object_url, i), params=params)
			if response.status_code != 200:
				print(response.status_code, response.reason, file=sys.stderr)
				break
			else:
				try:
					body = json.loads(response.text)
					if 'data' not in body or len(body['data']) == 0:
						break
					else:
						objects += body['data']
				except Exception as e:
					print('Got the following exception for request {}: {}'.format(response.url, e), file=sys.stderr)
					break

			# sleep for 4 seconds after each request because max allowed rate is low. (30 requests per minute)
			sleep(4)

		return objects

def main(args):
	data_collector = CoreAPIDataCollector(args.core_api_access_token)
	# assuming core api is NOT case sensitive
	articles = data_collector.get_articles_for_topics(['machine learning'], ['natural language processing', 'nlp', 'language model'])
	util.dump_json(articles, args.output_articles_json_path)

if __name__ == '__main__':
	import argparse

	parser = argparse.ArgumentParser()
	required_arguments = parser.add_argument_group('required arguments')
	required_arguments.add_argument('-t', '--core-api-access-token', action='store', required=True, default=None, dest='core_api_access_token', help='Core API access token.')
	required_arguments.add_argument('-o', '--output-articles-json-path', action='store', required=True, default=None, dest='output_articles_json_path', help='Path where articles json will get output.')

	args = parser.parse_args()
	main(args)