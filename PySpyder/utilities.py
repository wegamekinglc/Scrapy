# -*- coding: utf-8 -*-
u"""
Created on 2016-11-25
@author: cheng.li
"""

import time
import logging
import sqlalchemy


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

hedge_fund_db_settings = {'host': 'localhost',
                          'user': 'sa',
                          'pwd': 'we083826',
                          'db': 'hedge_fund',
                          'charset': 'utf8'}

exchange_db_settings = {'host': 'localhost',
                        'user': 'sa',
                        'pwd': 'we083826',
                        'db': 'exchange',
                        'charset': 'utf8'}


def create_engine(db_settings):
    return sqlalchemy.create_engine("mysql+pymysql://{user}:{passwd}@{host}/{db}?charset={charset}"
                                    .format(host=db_settings['host'],
                                            user=db_settings['user'],
                                            passwd=db_settings['pwd'],
                                            db=db_settings['db'],
                                            charset=db_settings['charset']))


def insert_table(data, field_names, table_name, db_settings):
    engine = create_engine(db_settings)
    data.columns = field_names
    data.to_sql(table_name, engine, if_exists='append', index=False)


def try_request(session, query_url, data=None, req_type='get', max_tries=5, sleep_seconds=10):
    tries = 0
    while True:
        try:
            if req_type == 'post':
                info_data = session.post(query_url, data=data)
            else:
                info_data = session.get(query_url)
            return info_data
        except ConnectionError:
            tries += 1
            if tries >= max_tries:
                raise
            spyder_logger.warning('retrying for the {0} times'.format(tries))
            time.sleep(sleep_seconds)
            continue
