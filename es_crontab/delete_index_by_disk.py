#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
磁盘容量超过设定阈值时，开始删除索引
从最早的索引数据开始删，直到数据所占磁盘容量小于阈值
按天分库格式: cc-*-2018.09.26 或 sc-*-2018.09.26
按年分库格式: cc-*-2018 或 sc-*-2018
每删除一天/一年的数据后，进行一次容量检查
用法：
crontab: 0 */6 * * *  python /{your_dir}/delete_index_by_disk.py >/dev/null 2>&1    # 每6小时执行一次脚本里的命令
"""
import os
import re
import time
import logging.handlers
from functools import reduce
from datetime import datetime, date, timedelta
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import *
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

CURRENT_DIR = reduce(lambda x, y: os.path.dirname(x), range(1), os.path.abspath(__file__))
LOG_DIR = os.path.join(CURRENT_DIR, 'logs')
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)
LOG_FILE = os.path.join(LOG_DIR, 'delete_index_by_disk.log')

handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=1024 * 1024, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(message)s')
handler.setFormatter(formatter)

logger = logging.getLogger('test')
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

# ----- 需要修改的参数 -----
es = Elasticsearch('127.0.0.1')
used_disk_pct_thresholds = 75
# ------------------------


def check_disk_usage(check_count):
    disk_warning = False
    res = es.cluster.allocation_explain(include_disk_info=True)
    nodes = res['cluster_info']['nodes']
    if check_count == 0:
        logger.info('检查磁盘容量')
    for node_id in nodes:
        node = nodes.get(node_id)
        used_disk_pct = max(node['most_available']['used_disk_percent'], node['least_available']['used_disk_percent'])
        if used_disk_pct > used_disk_pct_thresholds:
            disk_warning = True
            continue
        else:
            disk_warning = False
            logger.info('检查磁盘正常,当前磁盘容量{used_disk_pct}%\n'.format(used_disk_pct=used_disk_pct))
    if disk_warning:
        foremost_daily_suffix, foremost_yearly_suffix = get_foremost_suffix()
        es.indices.delete(index='*{suffix}'.format(suffix=foremost_daily_suffix), ignore=[400, 404])
        logger.info('当前磁盘容量{used_disk_pct}%，超过{used_disk_pct_thresholds}%,已删除{date}的数据'.format(
            used_disk_pct=used_disk_pct,
            used_disk_pct_thresholds=used_disk_pct_thresholds,
            date=foremost_daily_suffix))
        check_count += 1
        time.sleep(5)
        check_disk_usage(check_count)


def get_foremost_suffix():
    res = es.indices.get_settings(index='cc-*,sc-*')
    index_name_list = filter(lambda x: '-' in x, res.keys())
    suffix_list = []
    for index_name in index_name_list:
        suffix_list.append(index_name.split('-')[-1])
    suffix_list = list(set(suffix_list))
    daily_suffix_list = filter(lambda x: re.match(r'^(\d{4}\.\d{1,2}\.\d{1,2})$', x), suffix_list)
    yearly_suffix_list = filter(lambda x: re.match(r'^(\d{4})$', x), suffix_list)

    desc_daily_suffix_list = sorted(
        daily_suffix_list, reverse=True
    )
    desc_yearly_suffix_list = sorted(
        yearly_suffix_list, reverse=True
    )
    foremost_daily_suffix = desc_daily_suffix_list[-1]
    foremost_yearly_suffix = desc_yearly_suffix_list[-1]
    return foremost_daily_suffix, foremost_yearly_suffix


if __name__ == "__main__":
    check_disk_usage(check_count=0)
