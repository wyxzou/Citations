# Elasticsearch Python Tutorials

https://medium.com/naukri-engineering/elasticsearch-tutorial-for-beginners-using-python-b9cb48edcedc


# Elasticsearch Python API

https://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.Elasticsearch.index


# Elasticsearch Data Types

https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-data-types.html

s
# Useful cURL commands

To count number of entries in the database, use:

```
curl -X GET "https://search-ccf-x5pb62bjtwybf3wbaeyg2gby4y.us-east-2.es.amazonaws.com/aminer/_count?pretty" -H 'Content-Type: application/json' -d'
{
    "query": {
        "match_all": {}
    }
}
'
```

To nuke a index, use:

```
curl -XDELETE https://search-ccf-x5pb62bjtwybf3wbaeyg2gby4y.us-east-2.es.amazonaws.com/index-name-here
```