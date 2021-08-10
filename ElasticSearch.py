import ssl
import json
import elasticsearch
from elasticsearch.connection import create_ssl_context
from elasticsearch.helpers import scan
import urllib3
import sys
from elasticsearch import Elasticsearch
if not sys.warnoptions:
    import warnings
    warnings.simplefilter("ignore")

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class ElasticSearchGeneral:

    def __init__(self, es_url, es_username, es_password):
        """
        Initialize ElasticSearch client.
        """
        ssl_context = create_ssl_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        self.__es_client = elasticsearch.Elasticsearch(
            [es_url],
            http_auth=(es_username, es_password),
            scheme="https",
            ssl_context=ssl_context,
            verify_certs=False,
            ca_certs=False,
            use_ssl=True,
            timeout=300,
            max_retries=10,
            retry_on_timeout=True)


    def create_index(self, index, body=None):
        """
        Create an index. Body must be a dict with 'settings' and 'mappings' only.
        """
        r = self.__es_client.indices.create(index=index, body=body)
        print("Creating index: ", r)
        return r

    def delete_index(self, index):
        r = self.__es_client.indices.delete(index)
        print("Deleting index: name={}, response={}".format(index, r))
        return r

    def get_index(self, index):

        r = self.__es_client.indices.get(index=index)
        print(("getting index: name={}".format(index)))
        print(r)
        return r

    def exist_index(self, index):
        r = self.__es_client.indices.exists(index=index)
        # print("Index already exists: {}".formatrmat(r))
        return r

    def count(self, index, body=None):
        count = self.__es_client.count(body=body,index=index)["count"]
        print("Index docs count: {}".format(count))
        return count

    def transfer_data(self, source_index, target_index, query=None):
        """
        Transfer data from one index to another index.
        """
        if query:
            body = {
                "source": {"index": source_index,
                           "query": query['query']},
                "dest": {"index": target_index}
            }
        else:
            body = {
                "source": {"index": source_index},
                "dest": {"index": target_index}
            }
        print("Transfering data: source_index={}, target_index={}".format(source_index, target_index))
        r = self.__es_client.reindex(body=body,scroll="30m", slices="auto", timeout="60m")

        return r

    def transfer_data_from_remote_cluster(self, source_index, target_index, source_es_url, source_es_username,source_es_password, query=None):
        """
        Transfer data from one index to another index.
        """
        body = {
              "source": {
                "remote": {
                  "host": source_es_url,
                  "username": source_es_username,
                  "password": source_es_password
                },
                "index": source_index,
                "query": {
                        "bool": {
                          "must": [],
                          "filter": [
                            {
                              "match_all": {}
                            },
                            {
                              "range": {
                                "meta.event_time": {
                                  "gte": "2021-05-18T22:50:00.000Z",
                                  "lte": "2021-05-18T23:00:00.000Z",
                                  "format": "strict_date_optional_time"
                                }
                              }
                            }
                          ],
                          "should": [],
                          "must_not": []
                        }
                      }
                                  },
                                  "dest": {
                                    "index": target_index
                                  }
                                }

        if query is not None:
            body['source']['query'] = query
        r = self.__es_client.reindex(body=body, scroll="30m", slices="auto", timeout="60m")
        print("Transfering data: source_index={}, target_index={}, response={}".format(source_index, target_index, r))
        return r

    def exists_template(self, template_name):
        return self.__es_client.indices.exists_template(name=template_name)

    def create_index_template(self, template_name, file_path, env):
        """
        Create or update an index template.
        """
        with open(file_path, "r") as fp:
            template = json.load(fp)

        if env == "prod":
            template['settings']['number_of_replicas'] = 2
        exist = self.exists_template(template_name)
        if not exist:
            r = self.__es_client.indices.put_template(name=template_name, body=template)

            print("Creating index template: name={}, response={} from {}".format(template_name, r, env))
        else:
            r = self.__es_client.indices.put_template(name=template_name, body=template)
            print("Updating index template: name={}, response={} from {}".format(template_name, r, env))

    def get_all_templates(self):
        r = self.__es_client.cat.templates()
        print(r)
        return r

    def delete_index_template(self, template_name, env):

        if self.exists_template(template_name):
            r = self.__es_client.indices.delete_template(template_name)
            print(f"deleting index template={template_name} - {r} from {env}")
            return r
        else:
            return f"{template_name} does not exist"

    def get_field_mapping(self, fields, index):
        r = self.__es_client.indices.get_field_mapping(fields, index=index)
        print(r)
        return r

    def update_mapping(self, index, mappings):
        r = self.__es_client.indices.put_mapping(mappings, index=index)
        print("Updating mapping: index={}, response={}".format(index, r))
        return r

    def query(self):
        return

    def put_alias(self, index, name):
        if self.exist_index(index):
            r = self.__es_client.indices.put_alias(index=index, name=name, body={'is_write_index': True})
            print(f"Adding alias={name} pointing to {index} - {r}")
            return r
        else:
            return "Error: the index given does not exist"

    def exists_alias(self, name, index=None):
        r = self.__es_client.indices.exists_alias(name=name, index=index)
        print(r)
        return r

    def get_alias(self, index, name=None):
        r = self.__es_client.indices.get_alias(index=index, name=name)
        print(r)
        return r

    def delete_alias(self,index, name):
        if self.exists_alias(name=name, index=index):
            r = self.__es_client.indices.delete_alias(index=index, name=name)
            print(f"Deleting alias={name} from {index} - {r}")
            return r
        else:
            return f"{name} does not exist in {index}"

    def update_alias_pointer(self, index, name, old_index):
        self.delete_alias(old_index, name)
        self.put_alias(index, name)

    def get_index_names_from_templates(self):
        pass

    def get_template(self, template_name):
        r = self.__es_client.indices.get_template(template_name)
        return r

    def put_settings(self, settings, index):
        r = self.__es_client.indices.put_settings(body=settings, index=index)
        print(f"putting settings for index = {index}"
              f"{r}")

        return r

    def scan(self, index, query):
        r = scan(self.__es_client, index=index, query=query, scroll='100m', preserve_order=False,
                    size=10000, request_timeout=100, clear_scroll=False)
        return r

    def search_doc(self, body, index):

        r = self.__es_client.search(body=body, index=index)

        return r

    def get_doc(self, index, id):
        r = self.__es_client.get(index=index, id=id)
        print(r)
        return r

    def get_multiple_docs(self, body,index):
        r = self.__es_client.mget(body=body, index=index)
        # print(r)
        return r

    def scroll_search(self, **kwargs):
        r = self.__es_client.search(**kwargs)
        return r

    def scroll(self, **kwargs):
        r = self.__es_client.scroll(**kwargs)
        return r
    
    def update_doc(self, body, index, id):
        
        r = self.__es_client.update(index=index, doc_type="_doc", id=id, body=body)
        print(r)
        return r

    def insert_doc(self, doc, index, id):
        r = self.__es_client.index(index=index, id=id, body=doc)
        print(r)
        return r

    def delete_doc(self, index, id):
        r = self.__es_client.delete(index=index, id=id)
        print(r)
        return r

    def delete_by_query(self, index, body):
        r = self.__es_client.delete_by_query(index=index, body=body)
        print(r)
        return r




