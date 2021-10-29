# Citations

Recommends citations from aminer dataset https://www.aminer.org/citation. See slides Documentation/presentation.pdf.

## Core API Key
### https://core.ac.uk/api-keys/register/

## Core API documentation
### https://core.ac.uk/docs/

## Core API examples
### https://github.com/oacore/or2016-api-demo

## Requirements ##
### Please add dependencies to requirements.txt
### To install run: ``pip3 install -r requirements.txt``

## Starting elasticsearch
install from https://www.elastic.co/guide/en/elasticsearch/reference/current/install-elasticsearch.html

start elasticsearch using ``elasticsearch`` (from where the binary is located or put it in path)

run `python3 es_request.py` (update path to json file to your own)

use curl to hit endpoints  ``curl 'http://localhost:9200/id/'``

replace id with index name
