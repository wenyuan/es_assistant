#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
检查集群健康状态
1. 节点异常预警
2. 集群脑裂预警
用法：
crontab: 0 */1 * * *  python /{your_dir}/cluster_health_check.py >/dev/null 2>&1    # 每小时执行一次脚本里的命令
"""
import os
import re
import time
import json
import smtplib
from email.header import Header
from email.mime.text import MIMEText
from email.utils import parseaddr, formataddr
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
LOG_FILE = os.path.join(LOG_DIR, 'cluster_health_check.log')

handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=1024 * 1024, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(message)s')
handler.setFormatter(formatter)

logger = logging.getLogger('test')
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

# ----- 需要修改的参数 -----
# es_hosts写全所有节点ip
es_hosts = [
    '192.168.10.198',
    '192.168.10.200'
]
# email相关
email_title = 'xxxxxxx'
sender = 'sender@163.com'
password = 'password'
smtp_server = 'smtp.163.com'
smtp_port = 25
receivers = ['receiver1@163.com', 'receiver2@163.com']
# ------------------------


def check_cluster_health():
    cluster_info = []
    for es_host in es_hosts:
        try:
            es = Elasticsearch(es_host)
            res = es.cat.nodes(format='json')
            cluster_info.append({
                'node_ip': es_host,
                'cluster_ip_list': [node_info['ip'] for node_info in res]
            })
        except TransportError as e:
            if isinstance(e, ConnectionTimeout):
                logger.info('Read timed out!\n')
                suggestion = '检查ES是否繁忙'
                detail = """
                <div>异常节点IP: {es_host}</div>
                <div style='margin-bottom:5px'>错误提示: {reason}</div>
                """.format(es_host=es_host, reason='Read timed out!')
                send_email(email_title, suggestion, detail)
            elif isinstance(e, ConnectionError):
                logger.info('Elasticsearch connection refused!\n')
                suggestion = '检查ES是否已启动或运行异常'
                detail = """
                <div>异常节点IP: {es_host}</div>
                <div style='margin-bottom:5px'>错误提示: {reason}</div>
                """.format(es_host=es_host, reason='Elasticsearch connection refused!')
                send_email(email_title, suggestion, detail)
            else:
                logger.info('System err\n')
                suggestion = '未知，请登录服务器排查'
                detail = """
                <div>异常节点IP: {es_host}</div>
                <div style='margin-bottom:5px'>错误提示: {reason}</div>
                """.format(es_host=es_host, reason='System err!')
                send_email(email_title, suggestion, detail)
        except Exception as e:
            logger.info(e)
            logger.info('\n')

    split_cluster_info = []
    for node in cluster_info:
        if len(node['cluster_ip_list']) != len(es_hosts):
            split_cluster_info.append(node)
    if split_cluster_info:
        suggestion = '检查集群有无发生脑裂'
        detail = ''
        for node in split_cluster_info:
            detail += """
            <div>异常节点IP: {node_ip}</div>
            <div style='margin-bottom:5px'>当前节点所在集群: {cluster_ip_list}</div>
            """.format(node_ip=node['node_ip'], cluster_ip_list=','.join(node['cluster_ip_list']))
        send_email(email_title, suggestion, detail)


def send_email(subject, suggestion, detail):
    now_time = time.strftime('%Y-%m-%d %H:%M:%S')
    mail_msg = """
    <h1 style='margin-top:10px;margin-bottom:10px;text-align:center'>{subject}</h1>
    <hr>
    <h2 style='margin-top:0;margin-bottom:10px'>时间</h2>
    <div style='margin-left: 40px'>{now_time}</div>
    <hr>
    <h2 style='margin-top:0;margin-bottom:10px'>建议</h2>
    <div style='margin-left: 40px'>{suggestion}</div>
    <hr>
    <h2 style='margin-top:0;margin-bottom:10px'>详情</h2>
    <div style='margin-left: 40px'>{detail}</div>
    <hr>
    """.format(subject=subject, now_time=now_time, suggestion=suggestion, detail=detail)
    msg = MIMEText(mail_msg, 'html', 'utf-8')

    # msg['From'] = u'Monitor汇报人 <%s>' % sender
    # msg['To'] = u'Hibitom运维团队 <%s>' % 'receiver1@163.com,receiver2@163.com,receiver3@163.com'
    msg['From'] = Header('Monitor汇报人 <%s>' % sender, 'utf-8')
    msg['To'] = Header('Hibitom运维团队', 'utf-8')
    msg['Subject'] = Header(subject, 'utf-8')

    smtp = smtplib.SMTP()
    smtp.connect(smtp_server, smtp_port)
    # smtp.set_debuglevel(1)    # 打印和SMTP服务器交互的所有信息
    smtp.login(sender, password)
    smtp.sendmail(sender, receivers, msg.as_string())
    smtp.quit()


if __name__ == "__main__":
    check_cluster_health()
