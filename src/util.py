import json

def dump_json(data, json_file_path):
	with open(json_file_path, 'w') as json_file:
		json.dump(data, json_file, indent=4)

def load_json(json_file_path):
	data = None
	with open(json_file_path) as json_file:
		data = json.load(json_file)

	return data
