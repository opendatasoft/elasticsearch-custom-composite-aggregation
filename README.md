Elasticsearch custom composite aggregation
==========================================

This aggregations is a copy of elasticsearch composite aggregation (https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-composite-aggregation.html), but allow utilisation of multiple sources that can be nested and/or filtered.


In addition to the original composite aggregation, each source can allow two new parameters :

 - `nested_path`: define a nested path to retrieve nested document for this source.
 - `filter`: define a filter for a source. This parameter can take any elasticsearch filter 
 

For example :

```json
{
    "aggregations" : {
        "my_composite" : {
            "custom_composite": {
              "source": [
                {
                    "specie": {
                      "terms": {
                        "field": "fields.str_val.raw",
                        "nested_path": "fields",
                        "filter": {
                          "term": {
                            "fields.key": "specie"
                          }
                        }
                      }
                    }
                  },
                  {
                    "city": {
                      "terms": {
                        "field": "fields.long_val",
                        "nested_path": "fields",
                        "filter": {
                          "term": {
                            "fields.key": "city"
                          }
                        }
                      }
                    }
                  }
              ]
            }
        }
    }
}
```

Installation
------------

Plugin versions are available for (at least) all minor versions of Elasticsearch since 6.3.

The first 3 digits of plugin version is Elasticsearch versioning. The last digit is used for plugin versioning under an elasticsearch version.

To install it, launch this command in Elasticsearch directory replacing the url by the correct link for your Elasticsearch version (see table)
`./bin/elasticsearch-plugin install https://github.com/opendatasoft/elasticsearch-custom-composite-aggregation/releases/download/v6.3.2.0/custom-composite-aggregation-6.3.2.0.zip`

| elasticsearch version | plugin version | plugin url |
| --------------------- | -------------- | ---------- |
| 6.3.2 | 6.3.2.0 | https://github.com/opendatasoft/elasticsearch-composite-with-filter-aggregation/releases/download/v6.3.2.0/elasticsearch-composite-with-filter-aggregation-6.3.2.0.zip`

