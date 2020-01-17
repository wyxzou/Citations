import pickle
from gensim.models import FastText

from aminer.precision.utility import EnglishTextProcessor

etp = EnglishTextProcessor()
vec = pickle.load(open('vec.model', 'rb'))
fasttext = FastText.load('fasttext.model')
