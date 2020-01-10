import pickle
import sys
sys.path.append('../..')

from aminer.dataset import es_request
from aminer.precision.utility import EnglishTextProcessor
from gensim.models import Word2Vec
from nltk.tokenize import sent_tokenize
from sklearn.feature_extraction.text import TfidfVectorizer

# TODO: combine this with get_word_embeddings.py.

def iter_abstract_sentences_processed(es):
	def iter_abstracts(es):
		res = es.search(index='aminer', body={
			'_source': ['abstract'],
			'size': 10000,
			'query': {
				'match_all': {}
			}
		}, scroll='24h')
		scroll_id = res['_scroll_id']
		while res['hits']['hits'] and len(res['hits']['hits']) > 0:
			for hit in res['hits']['hits']:
				abstract = hit['_source']['abstract']
				yield abstract

			# use es scroll api
			res = es.scroll(scroll_id=scroll_id, scroll='24h', request_timeout=10)
			break

		es.clear_scroll(body={'scroll_id': scroll_id})

	etp = EnglishTextProcessor()
	for i, abstract in enumerate(iter_abstracts(es)):
		if i % 10000 == 0:
			print('Batch:', i // 10000)

		for sentence in sent_tokenize(abstract):
			yield etp(sentence)

def get_tfidf_vectorizer(es, vector_size=50, window_size=5, min_word_count=1, worker_threads=1, epochs=20):	
	vec = TfidfVectorizer()
	vec.fit_transform(iter_abstract_sentences_processed(es))
	return vec

def main():
	es = es_request.connect_elasticsearch()
	vec = get_tfidf_vectorizer(es)
	with open('vec.model', 'wb') as vec_pickle_file:
		pickle.dump(vec, vec_pickle_file)

if __name__ == '__main__':
	main()