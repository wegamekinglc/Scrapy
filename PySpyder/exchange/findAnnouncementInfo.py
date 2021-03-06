# -*- coding: utf-8 -*-
u"""
Created on 2016-10-24

@author: cheng.li
"""

import datetime as dt
import pandas as pd

import PySpyder.exchange.xshe as xshe
import PySpyder.exchange.xshg as xshg
from PySpyder.utilities import insert_table
from PySpyder.utilities import exchange_db_settings
from PySpyder.utilities import spyder_logger


def announcement_info(query_date):
    xshe_info = xshe.announcement(query_date)
    xshg_info = xshg.announcement(query_date)

    return pd.concat([xshe_info, xshg_info]).reset_index(drop=True)[['报告日期',
                                                                     '证券代码',
                                                                     '标题',
                                                                     'url',
                                                                     'updateTime',
                                                                     'exchangePlace']]


def exchange_announcement_info(ref_date):

    total_table = announcement_info(ref_date.strftime('%Y-%m-%d'))

    if total_table.empty:
        spyder_logger.info('No new data is available for {0}'.format(ref_date))
        return

    total_table.drop_duplicates(['url'], inplace=True)

    if not total_table.empty:
        insert_table(total_table,
                     ['reportDate',
                      'instrumentID',
                      'title',
                      'url',
                      'updateTime',
                      'exchangePlace'],
                     'announcement_info',
                     exchange_db_settings)


if __name__ == "__main__":
    data = exchange_announcement_info(dt.datetime(2015, 4, 2))
    print(data)