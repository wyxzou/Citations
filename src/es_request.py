from elasticsearch import Elasticsearch
import requests
import json

es = Elasticsearch(
    ['https://b5d9b60753c1475b813fa4416dec2cbe.us-east-1.aws.found.io'],
    http_auth=('elastic', 'YfCzcnEoTbbkbXV9zM5kH8MN'),
    scheme="https",
    port=9243
)

print(es.ping())