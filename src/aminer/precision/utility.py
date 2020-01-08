import nltk
import os
import re
import sys
sys.path.append('../..')

from nltk.corpus import stopwords, wordnet
from nltk.stem import WordNetLemmatizer

class EnglishTextProcessor():
    pos_tag_map = {
        "J": wordnet.ADJ,
        "N": wordnet.NOUN,
        "V": wordnet.VERB,
        "R": wordnet.ADV
    }
    def __init__(self, stop_words_file_path=None):
        self.stop = []
        if not stop_words_file_path or not os.path.exists(stop_words_file_path):
            self.stop = stopwords.words('english')
        else:
            with open(stop_words_file_path) as stop_words_file:
                self.stop = stop_words_file.read().split('\n')

        self.lemmatizer = None
        try:
            self.lemmatizer = WordNetLemmatizer()
        except LookupError:
            if nltk.download('wordnet'):
                self.lemmatizer = WordNetLemmatizer()
            else:
                print('Could not download \'wordnet\'. Try running with root priveleges.', file=sys.stderr)
                raise Exception

    def lemmatize(self, text):
        if self.lemmatizer == None:
            return text

        tokens = text.split()
        return ' '.join([self.lemmatizer.lemmatize(token, pos) for token, pos in\
                        zip(tokens, [self.pos_tag_map.get(pos[1][0].upper(), wordnet.NOUN) for pos in nltk.pos_tag(tokens)])])

    def __call__(self, text):
        # lowercase
        text_lower = text.lower()
        # replace special characters with spaces
        text_no_punc = re.sub(r'[^\w\s]|\_', ' ', text_lower)
        # lemmatize
        sentences_without_stop_words_lemmatized = []
        for sentence in text_no_punc.split('\n'):
            sentence_tokens = nltk.word_tokenize(sentence)
            # remove stop words
            sentence_tokens_without_stop_words = [token for token in sentence_tokens if token not in self.stop]
            # lemmatize words
            sentence_without_stop_words_lemmatized = self.lemmatize(' '.join(sentence_tokens_without_stop_words))
            if sentence_without_stop_words_lemmatized.strip() != '':
                sentences_without_stop_words_lemmatized += [sentence_without_stop_words_lemmatized]

        return '\n'.join(sentences_without_stop_words_lemmatized)
