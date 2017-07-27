from elasticsearch import Elasticsearch

es = Elasticsearch(['192.168.1.65:9200'])

class Wasl:

    def __init__(self):
        pass

    def make(self, wasl_query):
        query = {
            "query" : {
                "bool" :{
                    "must" : [],
                    "must_not" : [],
                    "should" : [],
                }
            },
            "aggs" : {}
        }

        