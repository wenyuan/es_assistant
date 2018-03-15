#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from functools import reduce

BASE_DIR = reduce(lambda x, y: os.path.dirname(x), range(2), os.path.abspath(__file__))
LOGGING_FILE_PATH = os.path.join(BASE_DIR, 'logs')

if not os.path.exists(LOGGING_FILE_PATH):
    os.makedirs(LOGGING_FILE_PATH)

LOGGING = {
    'disable_existing_loggers': True,
    'version': 1,
    'formatters': {
        'standard': {
            # INFO 2016-09-03 16:25:20,067 /home/ubuntu/mysite/views.py views.py views get 29: some info...
            'format': '%(levelname)s %(asctime)s %(pathname)s %(module)s %(funcName)s %(lineno)d: ' +
                      '%(message)s'
        },
        'simple': {
            # some info...
            'format': '%(message)s'
        },
    },
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'simple'
        },
        'doc_sender_handler': {
            'level': 'DEBUG',
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': os.path.join(LOGGING_FILE_PATH, 'doc_sender.log'),
            'maxBytes': 1024 * 1024 * 5,
            'backupCount': 5,
            'formatter': 'simple',
        }
    },
    'loggers': {
        'develop': {
            'handlers': ['console'],
            'level': 'DEBUG',
            'propagate': True,
        },
        'doc_sender': {
            'handlers': ['doc_sender_handler'],
            'level': 'DEBUG',
            'propagate': False
        }
    }
}


if __name__ == "__main__":
    print('hello, here is settings.py!')
