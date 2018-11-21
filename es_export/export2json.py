#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
从es中导出一部分数据
导出形式为[{},{},{}]; 转成字符串后写入json文件
开发测试用,如果数据量很大或者涉及到深度分页,请采用scroll-api
"""
import os
import json
from functools import reduce
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import *
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

# ----- 需要修改的参数 -----
es_host = '192.168.10.50'
index_name = "materials-baike"
output_file_name = index_name + '.json'
doc_from = 0
doc_size = 10000
# ------------------------

CURRENT_DIR = reduce(lambda x, y: os.path.dirname(x), range(1), os.path.abspath(__file__))
JSON_DIR = os.path.join(CURRENT_DIR, 'jsonfiles')
if not os.path.exists(JSON_DIR):
    os.makedirs(JSON_DIR)
OUTPUT_FILE_PATH = os.path.join(JSON_DIR, output_file_name)


def export_es_data():
    query_body = {
        "query": {
            "match_all": {}
        },
        "from": doc_from,
        "size": doc_size
    }
    try:
        es = Elasticsearch(es_host)
        response = es.search(index=index_name,
                             body=query_body,
                             request_timeout=300)
        print('hits: %s' % (response['hits']['total']))
        print('used_time(ms): %s' % (response['took']))
        if response['hits']['total'] > 0:
            doc_hits = response['hits']['hits']
            doc_list = []
            for hit in doc_hits:
                doc_list.append(hit['_source'])
            bulk2json(doc_list)
    except TransportError as e:
        if isinstance(e, ConnectionTimeout):
            print('Read timed out!')
        elif isinstance(e, ConnectionError):
            print('Elasticsearch connection refused!')
        else:
            print('System err')
    except Exception as e:
        print(e)


def bulk2json(content):
    with open(OUTPUT_FILE_PATH, "w") as f:
        json.dump(content, f, encoding="UTF-8", ensure_ascii=False)
        print(u"数据导出完成... %s" % OUTPUT_FILE_PATH)


if __name__ == "__main__":
    export_es_data()
