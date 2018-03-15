#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
从xxx.json格式文件中批量导入数据到es
存在json文件中数据形式必须为[{},{},{}]
"""
import sys
import json
import logging.config
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import *
from settings.settings import *
reload(sys)
sys.setdefaultencoding('utf-8')

logging.config.dictConfig(LOGGING)
JSON_PATH = os.path.join(BASE_DIR, 'doc_sender', 'jsons')
if not os.path.exists(JSON_PATH):
    os.makedirs(JSON_PATH)
logger = logging.getLogger('develop')

es = Elasticsearch('192.168.10.50')
index_name = "materials-baike"
data_type = 'materials'


class DocSender(object):

    def __init__(self):
        pass

    def bulk2es(self, name):
        json_file_path = os.path.join(JSON_PATH, name + '.json')
        doc_list = self.read_jsonfile(json_file_path)
        body = []
        for doc in doc_list:
            body.append({
                "_index": index_name,
                "_type": data_type,
                "_source": doc
            })
        try:
            helpers.bulk(es, body)
        except TransportError as e:
            if isinstance(e, ConnectionTimeout):
                logger.info('Read timed out!')
            elif isinstance(e, ConnectionError):
                logger.info('Elasticsearch connection refused!')
            else:
                logger.info('System err')

    @staticmethod
    def read_jsonfile(json_file_path):
        with open(json_file_path, 'r') as load_f:
            doc_list = json.load(load_f, encoding=None)
            # logger.info(json.dumps(doc_list,encoding='utf-8',ensure_ascii=False))
            return doc_list


if __name__ == "__main__":
   doc_sender = DocSender()
   doc_sender.bulk2es('A')
