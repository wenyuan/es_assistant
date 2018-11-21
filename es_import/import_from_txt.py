#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
从xxx.txt格式文件中批量导入数据到es
存在txt文件中数据形式必须为一行一个doc
从txt中读取方式为逐行读取
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
txt_file_name = 'A.txt'
request_body_size = 100
# ------------------------

CURRENT_DIR = reduce(lambda x, y: os.path.dirname(x), range(1), os.path.abspath(__file__))
TXT_DIR = os.path.join(CURRENT_DIR, 'txtfiles')
if not os.path.exists(TXT_DIR):
    os.makedirs(TXT_DIR)
TXT_FILE_PATH = os.path.join(TXT_DIR, txt_file_name)


def bulk2es():
    with open(TXT_FILE_PATH, 'r') as f:
        doc_list = []
        line = f.readline()
        while line:
            line = line.strip('\n')
            doc_content = json.loads(line, encoding='utf-8')
            doc_list.append(doc_content)
            if len(doc_list) >= request_body_size:
                send_data2es(doc_list)
                doc_list = []
            line = f.readline()
        if doc_list:
            send_data2es(doc_list)


def send_data2es(doc_list):
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
            print('Read timed out!')
        elif isinstance(e, ConnectionError):
            print('Elasticsearch connection refused!')
        else:
            print('System err')


if __name__ == "__main__":
    bulk2es()
