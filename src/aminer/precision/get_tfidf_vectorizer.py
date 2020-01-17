import json
import os
import pickle
import sys
sys.path.append('../..')

from aminer.dataset import es_request
from aminer.precision.utility import EnglishTextProcessor
from gensim.models import Word2Vec
from nltk.tokenize import sent_tokenize
from sklearn.feature_extraction.text import TfidfVectorizer

# TODO: combine this with get_word_embeddings.py.

def iter_abstract_sentences_processed(abstract_folder, mode='sentence'):
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

		if mode == 'sentence':
			for sentence in sent_tokenize(abstract):
				yield etp(sentence)
		elif mode == 'abstract':
			yield etp(abstract)

def get_tfidf_vectorizer(abstract_folder, vector_size=50, window_size=5, min_word_count=1, worker_threads=1, epochs=20):	
	vec = TfidfVectorizer()
	vec.fit_transform(iter_abstract_sentences_processed(abstract_folder))
	return vec

def main():
	vec = get_tfidf_vectorizer('abstract_dictionaries')
	with open('vec.model', 'wb') as vec_pickle_file:
		pickle.dump(vec, vec_pickle_file)

if __name__ == '__main__':
	main()