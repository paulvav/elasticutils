import json
import requests

class ElasticUtil:

    def __init__(self, index,es_url):
        self.es_url = es_url
        self.index = index
        self.headers = {'Content-Type': "application/json", 'cache-control': "no-cache"}

    # function to insert a document
    def insert(self, document, doc_id=""):
        headers = {'Content-Type': "application/json", 'cache-control': "no-cache"}
        esresponse = requests.request("put", self.es_url + self.index + "/" + doc_id, json=document, headers=headers)
        return esresponse

    # Update doc by id
    def update(self, document, doc_id=""):
        json_body = {"doc":document,"doc_as_upsert":True}
        esresponse = requests.request("post", self.es_url + self.index + "/" + doc_id+"/_update", json=json_body, headers=self.headers)
        return esresponse

    # Function to bulk insert documents into elastic search, ensure each document is prefaced with the {{index:{}} command
    def bulk_insert(self, formatted_documents):
        headers = {'Content-Type': "application/json", 'cache-control': "no-cache"}
        esresponse = requests.request("post", self.es_url + self.index + "/_bulk", data=formatted_documents,
                                      headers=headers)
        return esresponse

    # Formats result into a Bulk Import payload for ES
    def format(self, content, command="{\"index\":{}}"):
        formatted = ""
        for post in content:
            post = {"doc":post}
            formatted += command
            formatted += "\n"
            formatted += json.dumps(post)
            formatted += "\n"
        return formatted

    # Function to run basic match query
    def search_match(self,field,value,source=[]):
        es_query = {"_source": source,
                    "query": {"match": {field: value}}}
        es_url = self.es_url + self.index+"/_search"
        es_response = requests.request("GET", es_url, json=es_query).json()

        try:
            hit = es_response['hits']['hits'][0]['_source']
        except Exception as e:
            return {"exception":str(e)}

        return hit

    # Function to search for document via its _id
    def search_by_ids(self,value,source=[]):
        es_query = {"_source": source,
                    "query": {"ids": {"values": value}}}
        es_url = self.es_url + self.index+"/_search"
        es_response = requests.request("GET", es_url, json=es_query).json()

        try:
            hit = es_response['hits']['hits'][0]['_source']
        except Exception as e:
            return {"exception":str(e)}

        return hit

    # Match all documents in the index
    # Optional parameter: source - to include only a subset of fields
    # Optional parameter: size - to specify number of matches returned
    def match_all(self,source=[],size=150):
        search_url = self.es_url + self.index + "/_search"
        es_query = {"size":size,"query":{},"sort":{}}
        es_query['query'] = {"match_all":{}}

        if source != []:
            es_query['_source']=source

        es_response = requests.request("POST",search_url,json=es_query)
        return es_response

    # Search for all functions without a field
    def match_all_without(self,field,source=[]):
        search_url = self.es_url + self.index + "/_search"
        es_query = {"size": 150,"_source":source,"query": {"bool":{"must_not":{"exists":{"field":field}}}}, "sort": {}}
        es_response = requests.request("POST", search_url, json=es_query)
        return es_response


    # Update all documents in this index
    # Input: JSON field/values to update
    # Output: elasticsearch response
    def update_all(self,body):
        headers = {'Content-Type': "application/json", 'cache-control': "no-cache"}
        script_command = {"source":""}
        ctx_prefix = "ctx._source['"

        for line in body:
            line = ctx_prefix + str(line) + "'] = \"" + str(body[line]) + "\""
            script_command['source'] += line+";"
        script_command = script_command
        es_query = {"script":script_command,"query":{"match_all":{}}}
        es_response = requests.request("post",self.es_url+self.index+"/"+"_update_by_query",json=es_query,headers=headers)
        return es_response

