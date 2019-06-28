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
es_host = '127.0.0.1'
used_disk_pct_thresholds = 75
# 不参与老化的索引，空数组时候全部索引参与老化
# ex: ['']
white_list = ['cc-iprobe-1m_data']
# ------------------------


def check_disk_usage(check_count):
    try:
        es = Elasticsearch(es_host)
        res = es.nodes.stats(metric=['fs'])
        nodes = res['nodes']
        if check_count == 0:
            logger.info('检查磁盘容量')
        used_disk_pct_list = []
        for node_id in nodes:
            node = nodes.get(node_id)
            free_in_bytes = node['fs']['total']['free_in_bytes']
            total_in_bytes = node['fs']['total']['total_in_bytes']
            used_disk_pct = format(
                float((total_in_bytes - free_in_bytes)) / float(total_in_bytes) * 100,
                ".2f"
            )
            used_disk_pct_list.append(float(used_disk_pct))
        max_used_disk_pct = max(used_disk_pct_list)
        if max_used_disk_pct > used_disk_pct_thresholds:
            disk_warning = True
        else:
            disk_warning = False
        if disk_warning:
            foremost_daily_suffix, foremost_yearly_suffix = get_foremost_suffix(es)
            if foremost_daily_suffix:
                foremost_indices = [
                    item['index'] for item in
                    es.cat.indices(index='*{suffix}'.format(suffix=foremost_daily_suffix), format='json')
                ]
                keep_indices = set()
                for index in foremost_indices:
                    for white in white_list:
                        if index.startswith(white):
                            keep_indices.add(index)
                            continue
                drop_indices = set(foremost_indices) - keep_indices
                # print(','.join(drop_indices))
                es.indices.delete(index=','.join(drop_indices), ignore=[400, 404])
                # 未加白名单的delete方法
                # es.indices.delete(index='*{suffix}'.format(suffix=foremost_daily_suffix), ignore=[400, 404])
                logger.info('当前所有节点中,占据磁盘容量百分比最多的为{used_disk_pct}%，超过{used_disk_pct_thresholds}%,已删除{date}的数据'.format(
                    used_disk_pct=max_used_disk_pct,
                    used_disk_pct_thresholds=used_disk_pct_thresholds,
                    date=foremost_daily_suffix))
                check_count += 1
                time.sleep(5)
                check_disk_usage(check_count)
            else:
                logger.info('不存在或只存在一条按天分库的索引,不进行删除,请手动检查服务器磁盘消耗')
            # todo...
            # 目前没有按年分库的业务,如果有看情况添加(独立删除or结合按天分库的索引选择性删除)
        else:
            logger.info('检查磁盘正常,当前所有节点中,占据磁盘容量百分比最多的为{used_disk_pct}%\n'.format(used_disk_pct=max_used_disk_pct))
    except TransportError as e:
        if isinstance(e, ConnectionTimeout):
            logger.info('Read timed out!\n')
        elif isinstance(e, ConnectionError):
            logger.info('Elasticsearch connection refused!\n')
        else:
            logger.info('System err\n')
    except Exception as e:
        logger.info(e)
        logger.info('\n')


def get_foremost_suffix(es):
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
    foremost_daily_suffix = desc_daily_suffix_list[-1] if desc_daily_suffix_list else ''
    foremost_yearly_suffix = desc_yearly_suffix_list[-1] if desc_yearly_suffix_list else ''
    # 如果只有一天的数据,就不删按年份库的索引
    if len(foremost_daily_suffix) in [0, 1]:
        foremost_daily_suffix = ''
    # 如果只有一年的数据,就不删按年份库的索引
    if len(yearly_suffix_list) in [0, 1]:
        foremost_yearly_suffix = ''
    return foremost_daily_suffix, foremost_yearly_suffix


if __name__ == "__main__":
    check_disk_usage(check_count=0)
