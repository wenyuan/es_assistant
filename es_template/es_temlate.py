#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
创建template
包含settings和mappings
https://www.jianshu.com/p/1f67e4436c37
"""
import json
import requests

# ----- 需要修改的参数 -----
host = '192.168.10.50'
index_name = "materials-baike"
data_type = 'materials'
# ------------------------


class EsTemplate(object):

    def __init__(self):
        pass

    def set_template(self):
        template_name = index_name + '-template'
        body = {
            "order": 1,                                   # 模板优先级
            "template": index_name + '*',                # 模板匹配的名称格式
            "settings": {                                # 索引设置
                "index": {
                    "refresh_interval": "10s",
                    "number_of_shards": 2,
                    "number_of_replicas": 0,
                    "routing.allocation.include._ip": "192.168.1.126,192.168.1.127",
                    "routing.allocation.total_shards_per_node": 1
                }
            },
            "mappings": {},                              # 索引中各字段的映射定义
            "aliases": {}                                # 索引的别名
        }
        self.send_request(template_name, body)

    @staticmethod
    def send_request(template_name, body):
        url = "http://{0}:9200/_template/{1}".format(host, template_name)
        r = requests.get(url)
        if r.status_code == 404:
            r = requests.put(url, data=json.dumps(body), headers={'content-type': 'application/json'})
            print(r.text)
            if r.status_code == 200:
                print('put template %s success!' % template_name)
            else:
                print('put template %s failed!' % template_name)
        elif r.status_code == 200:
            print('template %s exists!' % template_name)
        else:
            print('get template %s error!' % template_name)


if __name__ == "__main__":
    es_template = EsTemplate()
    es_template.set_template()
