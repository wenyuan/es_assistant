#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
从xxx.json格式文件中批量导入数据到es
存在json文件中数据形式必须为[{},{},{}]
开发测试用,建议1w以下的数据量
"""
import os
import sys
import json
from functools import reduce
from elasticsearch import Elasticsearch, helpers
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
data_type = 'materials'
# ------------------------


class EsImport(object):

    def __init__(self):
        pass

    def bulk2es(self, name):
        json_file_path = os.path.join(JSON_PATH, name + '.json')
        doc_list = self.read_jsonfile(json_file_path)
        print(u"数据导入完成... 共%s条" % len(doc_list))
        body = []
        for doc in doc_list:
            body.append({
                "_index": index_name,
                "_type": data_type,
                "_source": doc
            })
        try:
            success, failed = helpers.bulk(es, body)
            print('success: %s, failed: %s' % (success, failed))
        except TransportError as e:
            if isinstance(e, ConnectionTimeout):
                print('Read timed out!')
            elif isinstance(e, ConnectionError):
                print('Elasticsearch connection refused!')
            else:
                print('System err')

    @staticmethod
    def read_jsonfile(json_file_path):
        with open(json_file_path, 'r') as load_f:
            doc_list = json.load(load_f, encoding=None)
            # print(json.dumps(doc_list,encoding='utf-8',ensure_ascii=False))
            return doc_list


if __name__ == "__main__":
    es_import = EsImport()
    es_import.bulk2es('A')
