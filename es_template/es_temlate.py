#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
创建template
包含settings和mappings
https://www.jianshu.com/p/1f67e4436c37
"""
import json
import requests

# ---------- 需要修改的参数 ----------
es_host = '192.168.10.50'
template_name = 'dbmonitor-oracle-template'
index_name = 'cc-dbmonitor-oracle-*'
doc_type = 'oracle'
# ---------------------------------


def set_template():
    body = {
        "order": 1,
        "template": template_name,
        "settings": {
            "index": {
                "refresh_interval": "10s",
                "number_of_shards": 2,
                "number_of_replicas": 0,
                "routing.allocation.include._ip": "192.168.1.126,192.168.1.127",
                "routing.allocation.total_shards_per_node": 1
            }
        },
        "mappings": {
            doc_type: {
                'dynamic_templates': [
                    {
                        'string_fields': {
                            'match': '*',
                            'match_mapping_type': 'string',
                            'mapping': {
                                'type': 'keyword'
                            }
                        }
                    }
                ],
                'properties': {
                    '@timestamp': {
                        'format': 'strict_date_optional_time||epoch_millis',
                        'type': 'date'
                    }
                }
            }
        },
        "aliases": {}
    }

    # es execute
    url = "http://{es_host}:9200/_template/{template_name}".format(
        es_host=es_host,
        template_name=template_name
    )
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
    set_template()
