import sys
sys.path.append('../..')

from aminer.dataset import es_request
from aminer.precision.utility import EnglishTextProcessor
from gensim.models import Word2Vec
from nltk.tokenize import sent_tokenize

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

		es.clear_scroll(body={'scroll_id': scroll_id})

	etp = EnglishTextProcessor()
	for i, abstract in enumerate(iter_abstracts(es)):
		if i % 10000 == 0:
			print('Batch:', i // 10000)

		for sentence in sent_tokenize(abstract):
			yield etp(sentence).split()

def get_embeddings(es, vector_size=50, window_size=5, min_word_count=1, worker_threads=1, epochs=20):	
	embeddings = Word2Vec(size=vector_size, window=window_size, min_count=min_word_count, workers=worker_threads)
	embeddings.build_vocab(sentences=iter_abstract_sentences_processed(es))
	embeddings.train(sentences=iter_abstract_sentences_processed(es), epochs=epochs, total_examples=embeddings.corpus_count)
	return embeddings

def main():
	es = es_request.connect_elasticsearch()
	embeddings = get_embeddings(es)
	embeddings.save("output.model")

if __name__ == '__main__':
	main()