import json
import numpy as np
import os
import random

from pytorch_pretrained_bert.tokenization import BertTokenizer
from util import EnglishTextProcessor, dump_json, load_json

class Dataset():
	def __init__(self, pretrained_bert_folder_path, processed_abstract_folder_path, references_folder_path, max_file_body_length):
		self.id_to_references = {}
		for file_path in os.listdir(references_folder_path):
			with open(os.path.join(references_folder_path, file_path)) as references_file:
				self.id_to_references.update(json.load(references_file))

		self.id_to_abstract = {}
		for file_path in os.listdir(processed_abstract_folder_path):
			with open(os.path.join(processed_abstract_folder_path, file_path)) as abstract_file:
				self.id_to_abstract.update(json.load(abstract_file))

		self.bert_tokenizer = BertTokenizer.from_pretrained(pretrained_bert_folder_path)

	@staticmethod
	def truncate_sequence_pair(sequence_a, sequence_b, max_sequence_length):
		while len(sequence_a) + len(sequence_b) > max_sequence_length:
			sequence_to_truncate = sequence_a
			if len(sequence_b) > len(sequence_a):
				sequence_to_truncate = sequence_b

			if random.random() >= 0.5:
				sequence_to_truncate.pop(-1)
			else:
				sequence_to_truncate.pop(0)

		return sequence_a, sequence_b

	@staticmethod
	def create_masked_lm_labels(bert_tokenizer, sequence, masked_lm_probability, max_predictions_per_sequence):
		vocabulary = list(bert_tokenizer.vocab.keys())
		num_predictions = min(max_predictions_per_sequence, min(1, int(len(sequence) * masked_lm_probability)))
		indices = [i for i in range(len(sequence))]
		random.shuffle(indices)
		prediction_count = 0
		masked_lm_label_ids = [-1 for _ in range(len(sequence))]
		for index in indices:
			if prediction_count >= num_predictions:
				break

			if sequence[index] != '[CLS]' or sequence[index] != '[SEP]':
				prediction_count += 1
				masked_lm_label_ids[index] = bert_tokenizer.convert_tokens_to_ids([sequence[index]])[0]
				if random.random() >= 0.2:
					sequence[index] = '[MASK]'
				elif random.random() >= 0.5:
					sequence[index] = random.choice(vocabulary)

		return masked_lm_label_ids

	@staticmethod
	def create_example(bert_tokenizer, id, sentence_0_tokens, sentence_1_tokens, is_next, max_sequence_length, masked_lm_probability, max_predictions_per_sequence):
		sentence_0_tokens, sentence_1_tokens = Dataset.truncate_sequence_pair(sentence_0_tokens, sentence_1_tokens, max_sequence_length)
		example_tokens = ['[CLS]'] + sentence_0_tokens + ['[SEP]'] + sentence_1_tokens + ['[SEP]']
		segment_ids = [0 for i in range(len(sentence_0_tokens) + 2)] + [1 for i in range(len(sentence_1_tokens) + 1)]
		label_ids = Dataset.create_masked_lm_labels(bert_tokenizer, example_tokens, masked_lm_probability, max_predictions_per_sequence)
		is_next = is_next
		return {
			'example_id': id,
			'input_tokens': example_tokens,
			'segment_ids': segment_ids,
			'masked_label_ids': label_ids,
			'is_next': is_next
		}

	def get_abstract_next_sentence_data(self, id, abstract, references, max_sequence_length, masked_lm_probability, max_predictions_per_sequence, all_negative_examples):
		max_sequence_length -= 3
		assert max_sequence_length > 0

		examples = []
		positive_examples_created = 0
		abstract_query_tokens = self.bert_tokenizer.tokenize(abstract)
		for reference_id in references:
			# since we are looking at the latest source code, a file may have been deleted
			query_tokens = abstract_query_tokens.copy()
			reference_abstract = self.id_to_abstract[reference_id]
			reference_tokens = self.bert_tokenizer.tokenize(reference_abstract)
			example = Dataset.create_example(self.bert_tokenizer, id, query_tokens, reference_tokens, True, max_sequence_length, masked_lm_probability, max_predictions_per_sequence)
			examples += [example]
			positive_examples_created += 1

		# randomly create negative examples
		shuffled_ids = list(self.id_to_abstract.keys())
		random.shuffle(shuffled_ids)
		negative_examples_created = 0
		index = 0
		while index < len(shuffled_ids):
			if not all_negative_examples and negative_examples_created >= positive_examples_created:
				break

			negative_reference_id = shuffled_ids[index]
			if negative_reference_id not in references:
				query_tokens = abstract_query_tokens.copy()
				negative_reference_abstract = self.id_to_abstract[negative_reference_id]
				negative_reference_tokens = self.bert_tokenizer.tokenize(negative_reference_abstract)
				example = Dataset.create_example(self.bert_tokenizer, id, query_tokens, negative_reference_tokens, False, max_sequence_length, masked_lm_probability, max_predictions_per_sequence)
				examples += [example]
				negative_examples_created += 1

			index += 1

		return examples

	def get_next_sentence_data(self, max_sequence_length, masked_lm_probability, max_predictions_per_sequence, all_negative_examples=False, max_dataset_size=1000000):
		# convert data to format expected by bert
		next_sentence_data = []
		for id, abstract in self.id_to_abstract:
			references = self.id_to_references[id]
			examples = self.get_abstract_next_sentence_data(id, abstract, references, max_sequence_length, masked_lm_probability, max_predictions_per_sequence, all_negative_examples)
			next_sentence_data.extend(examples)
			if len(next_sentence_data) > max_dataset_size:
				break

		return next_sentence_data

def generate_dataset(pretrained_bert_folder_path, abstract_folder_path, references_folder_path, max_sequence_length, masked_lm_probability=0.2, max_predictions_per_sequence=5):
	dataset = Dataset(pretrained_bert_folder_path, abstract_folder_path, references_folder_path, max_sequence_length)
	next_sentence_data = dataset.get_next_sentence_data(max_sequence_length, 0.2, 5)
	return next_sentence_data

def main(args):
	next_sentence_data = generate_dataset(args.pretrained_bert_folder_path, args.abstract_folder_path, args.references_folder_path, args.max_sequence_length)
	dump_json(args.output_path, next_sentence_data)

if __name__ == '__main__':
	import argparse

	parser = argparse.ArgumentParser()
	required_arguments = parser.add_argument_group('required arguments')
	required_arguments.add_argument('-b', '--pretrained-bert-folder-path', action='store', required=True, default=None, dest='pretrained_bert_folder_path', help='Path to folder containing pretrained bert config.json, .bin and vocab.txt.')
	required_arguments.add_argument('-a', '--abstract-folder-path', action='store', required=True, default=None, dest='abstract_folder_path', help='Path to folder containing abstract batches.')
	required_arguments.add_argument('-r', '--references-folder-path', action='store', required=True, default=None, dest='references_folder_path', help='Path to folder containing references batches.')
	required_arguments.add_argument('-o', '--output-path', action='store', required=True, default=None, dest='output_path', help='Path to json file where next sentence prediction data will be output.')

	optional_arguments = parser.add_argument_group('optional arguments')
	optional_arguments.add_argument('-m', '--max-sequence-length', action='store', required=False, default=256, type=int, dest='max_sequence_length', help='Maximum sequence length of examples.')
	
	args = parser.parse_args()
	main(args)