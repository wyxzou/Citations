import json
import numpy as np
import pandas as pd
import torch

from pytorch_pretrained_bert.modeling import BertForPreTraining
from pytorch_pretrained_bert.optimization import BertAdam
from pytorch_pretrained_bert.tokenization import BertTokenizer
from torch.utils.data import Dataset, DataLoader, RandomSampler
from tqdm import tqdm
from util import pad_array, load_json

class SentencePairDataset(Dataset):
	class InputFeatures():
		def __init__(self, input_ids, input_mask, segment_ids, label_ids, is_next):
			self.input_ids = input_ids
			self.input_mask = input_mask
			self.segment_ids = segment_ids
			self.label_ids = label_ids
			self.is_next = is_next
	
	def __init__(self, tokenizer, sentence_pair_data, max_sequence_length):
		self.data_points = []
		self.tokenizer = tokenizer
		self.max_sequence_length = max_sequence_length
		for example in sentence_pair_data:
			input_ids, input_mask, segment_ids, label_ids, is_next = SentencePairDataset.convert_example_to_features(example['input_tokens'],
																													 example['segment_ids'],
																													 example['masked_label_ids'],
																													 example['is_next'],
																													 max_sequence_length,
																													 self.tokenizer)
			self.data_points += [SentencePairDataset.InputFeatures(input_ids, input_mask, segment_ids, label_ids, is_next)]

	@staticmethod
	def convert_example_to_features(input_tokens, segment_ids, masked_label_ids, is_next, max_sequence_length, tokenizer):
		def convert_tokens_to_ids(input_tokens, tokenizer):
			input_ids = np.zeros(len(input_tokens))
			input_ids[:len(input_tokens)] = tokenizer.convert_tokens_to_ids(input_tokens)
			return input_ids

		input_ids = np.array(pad_array(convert_tokens_to_ids(input_tokens, tokenizer), max_sequence_length, 0), dtype=np.int32)
		input_mask = np.array(pad_array([1 for _ in range(input_ids.size)], max_sequence_length, 0), dtype=np.bool)
		segment_ids = np.array(pad_array(segment_ids, max_sequence_length, 0), dtype=np.bool)
		label_ids = np.array(pad_array(masked_label_ids, max_sequence_length, 0), dtype=np.int32)
		is_next = np.array(is_next, dtype=np.bool)
		return input_ids, input_mask, segment_ids, label_ids, is_next

	def __len__(self):
		return len(self.data_points)

	def __getitem__(self, index):
		input_features = self.data_points[index]
		return torch.tensor(input_features.input_ids, dtype=torch.int64),\
			   torch.tensor(input_features.segment_ids, dtype=torch.int64),\
			   torch.tensor(input_features.input_mask, dtype=torch.int64),\
			   torch.tensor(input_features.label_ids, dtype=torch.int64),\
			   torch.tensor(input_features.is_next, dtype=torch.int64)

class BertFineTuner():
	def __init__(self, input_model_folder_path):
		self.model = BertForPreTraining.from_pretrained(input_model_folder_path)
		self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

	def fine_tune(self, dataset, lr, epochs, gradient_accumulation_steps, batch_size, max_steps):
		self.model.to(self.device)
		parameters_without_decay = ['bias', 'LayerNorm.bias', 'LayerNorm.weight']
		params = [{'params': [param for name, param in self.model.named_parameters() if not any(decay_param_name in name for decay_param_name in parameters_without_decay)], 'weight_decay': 0.01},
				  {'params': [param for name, param in self.model.named_parameters() if any(decay_param_name in name for decay_param_name in parameters_without_decay)], 'weight_decay': 0.0}]
		optimizer = BertAdam(params, lr=lr, warmup=0.1, t_total=int(epochs * len(dataset) / batch_size / gradient_accumulation_steps))

		self.model.train()

		random_sampler = RandomSampler(dataset)
		data_loader = DataLoader(dataset, batch_size=batch_size, sampler=random_sampler)
		for _ in range(epochs):
			for i, batch in tqdm(enumerate(data_loader)):
				if i >= max_steps:
					break

				input_ids, segment_ids, input_mask, label_ids, is_next = tuple([feature.to(self.device) for feature in batch])
				loss = self.model(input_ids, segment_ids, input_mask, label_ids, is_next)
				print(loss.item())
				loss /= gradient_accumulation_steps
				loss.backward()
				if (i + 1) % gradient_accumulation_steps == 0:
					optimizer.step()
					optimizer.zero_grad()

	def save(self, output_model_path, output_config_path):
		torch.save(self.model.state_dict(), output_model_path)
		self.model.config.to_json_file(output_config_path)

def main(args):
	tokenizer = BertTokenizer.from_pretrained(pretrained_model_name_or_path=args.pretrained_bert_folder_path)
	sentence_pair_data = load_json(args.data_json_path)
	dataset = SentencePairDataset(tokenizer, sentence_pair_data, args.max_sequence_length)
	fine_tuner = BertFineTuner(args.pretrained_bert_folder_path)
	fine_tuner.fine_tune(dataset, args.learning_rate, args.epochs, args.gradient_accumulation_steps, args.batch_size, args.max_steps)
	fine_tuner.save(args.fine_tuned_bert_output_path, 'bert_config.json')

if __name__ == '__main__':
	import argparse

	parser = argparse.ArgumentParser()
	required_arguments = parser.add_argument_group('required arguments')
	required_arguments.add_argument('-b', '--pretrained-bert-folder-path', action='store', required=True, default=None, dest='pretrained_bert_folder_path', help='Path to folder containing pretrained bert config.json, .bin and vocab.txt.')
	required_arguments.add_argument('-d', '--data-json-path', action='store', required=True, default=None, dest='data_json_path', help='Path to json file containing data.')
	required_arguments.add_argument('-o', '--fine-tuned-bert-output-path', action='store', required=True, default=None, dest='fine_tuned_bert_output_path', help='Path where model will be saved after fine tuning.')

	optional_arguments = parser.add_argument_group('optional arguments')
	optional_arguments.add_argument('-m', '--max-sequence-length', action='store', required=False, default=64, type=int, dest='max_sequence_length', help='Maximum sequence length of examples.')
	optional_arguments.add_argument('-g', '--gradient-accumulation-steps', action='store', required=False, default=1, type=int, dest='gradient_accumulation_steps', help='Accumulate loss and make backward call after N steps.')
	optional_arguments.add_argument('-lr', '--learning-rate', action='store', required=False, default=0.1, type=float, dest='learning_rate', help='Optimizer learning rate.')
	optional_arguments.add_argument('-e', '--epochs', action='store', required=False, default=1, type=int, dest='epochs', help='Number of epochs in training.')
	optional_arguments.add_argument('-s', '--batch-size', action='store', required=False, default=10, type=int, dest='batch_size', help='Size of a batch while training.')
	optional_arguments.add_argument('-steps', '--max-steps', action='store', required=False, default=400, type=int, dest='max_steps', help='Max number of steps in each epoch.')
	
	args = parser.parse_args()
	main(args)