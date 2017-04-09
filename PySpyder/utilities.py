# -*- coding: utf-8 -*-
u"""
Created on 2016-11-25
@author: cheng.li
"""

import logging


class CustomLogger(object):
    def __init__(self,
                 logger_name,
                 log_level):
        self.logger = logging.getLogger(logger_name)
        ch = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)
        self.set_level(log_level)

    def set_level(self, log_level):
        if log_level.lower() == "info":
            self.logger.setLevel(logging.INFO)
        elif log_level.lower() == "warning":
            self.logger.setLevel(logging.WARNING)
        elif log_level.lower() == 'critical':
            self.logger.setLevel(logging.CRITICAL)
        elif log_level.lower() == 'debug':
            self.logger.setLevel(logging.DEBUG)

    def info(self, msg):
        self.logger.info(msg)

    def warning(self, msg):
        self.logger.warning(msg)

    def critical(self, msg):
        self.logger.critical(msg)

    def debug(self, msg):
        self.logger.debug(msg)

spyder_logger = CustomLogger('PY_SPYDER_LOGGER', 'info')