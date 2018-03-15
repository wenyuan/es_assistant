#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging.config
from settings.settings import *

logging.config.dictConfig(LOGGING)
logger = logging.getLogger('doc_spliter')


if __name__ == "__main__":
    logger.info('hello, here is main.py')
