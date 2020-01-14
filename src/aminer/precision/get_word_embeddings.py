import json
import os
import sys
sys.path.append('../..')

from aminer.dataset import es_request
from aminer.precision.utility import EnglishTextProcessor
from gensim.models import Word2Vec
from nltk.tokenize import sent_tokenize

def iter_abstracts_es(es):
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

def iter_abstract_sentences_processed(abstract_folder):
	def iter_abstracts(abstract_folder):
		json_file_names = sorted(os.listdir(abstract_folder), key=lambda x: int(x.split('_')[-1].split('.')[0]))
		for file_name in json_file_names:
			file_path = os.path.join(abstract_folder, file_name)
			try:
				id_to_abstract_dict = None
				with open(file_path) as id_to_abstract_json_file:
					id_to_abstract_dict = json.load(id_to_abstract_json_file)
				
				for id in id_to_abstract_dict:
					yield id_to_abstract_dict[id]

			except ValueError:
				continue

	etp = EnglishTextProcessor()
	for i, abstract in enumerate(iter_abstracts(abstract_folder)):
		if i % 10000 == 0:
			print('Batch:', i // 10000)

		for sentence in sent_tokenize(abstract):
			yield etp(sentence).split()

def get_embeddings(abstract_folder, vector_size=50, window_size=5, min_word_count=1, worker_threads=1, epochs=20):	
	embeddings = Word2Vec(size=vector_size, window=window_size, min_count=min_word_count, workers=worker_threads)
	embeddings.build_vocab(sentences=iter_abstract_sentences_processed(abstract_folder))
	embeddings.train(sentences=iter_abstract_sentences_processed(abstract_folder), epochs=epochs, total_examples=embeddings.corpus_count)
	return embeddings

def main():
	embeddings = get_embeddings('abstract_dictionaries')
	embeddings.save("embeddings.model")

if __name__ == '__main__':
	main()