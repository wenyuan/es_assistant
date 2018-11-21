#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
从xxx.json格式文件中批量导入数据到es
存在json文件中数据形式必须为[{},{},{}]
开发测试用,建议1w以下的数据量
"""
import os
import json
from functools import reduce
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import *
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

# ----- 需要修改的参数 -----
es_host = '192.168.10.50'
index_name = 'materials-baike'
doc_type = 'xxx'
json_file_name = 'A.json'
# ------------------------

CURRENT_DIR = reduce(lambda x, y: os.path.dirname(x), range(1), os.path.abspath(__file__))
JSON_DIR = os.path.join(CURRENT_DIR, 'jsonfiles')
if not os.path.exists(JSON_DIR):
    os.makedirs(JSON_DIR)
JSON_FILE_PATH = os.path.join(JSON_DIR, json_file_name)


def bulk2es():
    json_file_path = JSON_FILE_PATH
    doc_list = read_jsonfile(json_file_path)
    print(u"数据导入完成... 共%s条" % len(doc_list))
    actions = []
    for doc in doc_list:
        actions.append({
            '_op_type': 'index',
            '_index': index_name,
            '_type': doc_type,
            '_source': doc
        })
    try:
        es = Elasticsearch(es_host)
        success, failed = helpers.bulk(client=es, actions=actions)
        print('Bulk success: %s, failed: %s' % (success, failed))
    except TransportError as e:
        if isinstance(e, ConnectionTimeout):
            print('Read timed out')
        elif isinstance(e, ConnectionError):
            print('Elasticsearch connection refused')
        else:
            print('System err')


def read_jsonfile(json_file_path):
    with open(json_file_path, 'r') as load_f:
        doc_list = json.load(load_f, encoding=None)
        # print(json.dumps(doc_list,encoding='utf-8',ensure_ascii=False))
        return doc_list


if __name__ == "__main__":
    bulk2es()
