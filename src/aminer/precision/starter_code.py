import pickle

from utility import EnglishTextProcessor
from gensim.models import FastText

etp = EnglishTextProcessor()
vec = pickle.load(open('vec.model', 'rb'))
fasttext = FastText.load('fasttext.model')
