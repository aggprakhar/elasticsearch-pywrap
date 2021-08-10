from ElasticSearch import ElasticSearchGeneral
from utils.Globals import *
import json
import ssl
import elasticsearch
from elasticsearch import Elasticsearch
from elasticsearch.connection import create_ssl_context
ssl_context = create_ssl_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

path = ""

templates = [x.replace(".json", "") for x in os.listdir(f"{path}") if x.endswith("json")]
environments = ['dev', 'prod']

for env in environments:
    if env=='prod':
        es = ElasticSearchGeneral(f"url_{env}", "username", "password")
        for template in templates:
            if template=="":

                es.create_index_template(template, f"{path}/{template}.json", env)

                with open(f"{path}/{template}.json", "r") as file:
                    data = json.load(file)

                    print(data['index_patterns'])
                    for idx in data['index_patterns']:
                        if idx == "":
                            index = idx.replace("*", "_dummy")
                            alias = index.replace("index", "alias")
                            print(index, alias)
                            es.create_index(f"{env}{index}")
                            print(es.get_index(f"{env}{index}"))
                            es.put_alias(f"{env}{index}", f"{env}{alias}")
                            es.get_alias(f"{env}{index}", f"{env}{alias}")

