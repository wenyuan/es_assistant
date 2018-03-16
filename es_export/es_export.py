#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
从es中导出一部分数据
导出形式为[{},{},{}]; 转成字符串后写入json文件
开发测试用,如果数据量很大或者涉及到深度分页,请采用scroll-api
"""
import os
import sys
import json
from functools import reduce
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import *
reload(sys)
sys.setdefaultencoding('utf-8')

CURRENT_DIR = reduce(lambda x, y: os.path.dirname(x), range(1), os.path.abspath(__file__))
JSON_PATH = os.path.join(CURRENT_DIR, 'jsons')
if not os.path.exists(JSON_PATH):
    os.makedirs(JSON_PATH)

# ----- 需要修改的参数 -----
es = Elasticsearch('192.168.10.50')
index_name = "materials-baike"
json_file_path = os.path.join(JSON_PATH, index_name + '.json')
doc_from = 0
doc_size = 10000
# ------------------------


class EsExport(object):

    def __init__(self):
        pass

    def export_es_data(self):
        query_body = {
            "query": {
                "match_all": {}
            },
            "from": doc_from,
            "size": doc_size
        }
        try:
            response = es.search(index=index_name,
                                 body=query_body,
                                 request_timeout=300)
            print('hits: %s' % (response['hits']['total']))
            print('used_time(ms): %s' % (response['took']))
        except TransportError as e:
            if isinstance(e, ConnectionTimeout):
                print('Read timed out!')
            elif isinstance(e, ConnectionError):
                print('Elasticsearch connection refused!')
            else:
                print('System err')
        except Exception as e:
            print(e)
        if response['hits']['total'] > 0:
            doc_hits = response['hits']['hits']
            doc_list = []
            for hit in doc_hits:
                doc_list.append(hit['_source'])
            self.bulk2json(doc_list)

    @staticmethod
    def bulk2json(content):
        with open(json_file_path, "w") as f:
            json.dump(content, f, encoding="UTF-8", ensure_ascii=False)
            print(u"数据导出完成... %s" % json_file_path)


if __name__ == "__main__":
    es_export = EsExport()
    es_export.export_es_data()
