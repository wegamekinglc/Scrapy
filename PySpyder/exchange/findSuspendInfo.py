# -*- coding: utf-8 -*-
u"""
Created on 2016-10-25

@author: cheng.li
"""

import datetime as dt
import pandas as pd

from PyFin.api import isBizDay
import PySpyder.exchange.xshe as xshe
import PySpyder.exchange.xshg as xshg
from PySpyder.utilities import insert_table
from PySpyder.utilities import exchange_db_settings
from PySpyder.utilities import create_engine
from PySpyder.utilities import spyder_logger


def find_latest_date():
    engine = create_engine(exchange_db_settings)
    sql = 'select effectiveDate from suspend_info'
    exist_data = pd.read_sql(sql, engine).sort_values('effectiveDate')
    if len(exist_data) > 0:
        return exist_data.iloc[len(exist_data) - 1]['effectiveDate']
    else:
        return pd.Timestamp('2015-01-01')


def suspend_info(query_date):
    xshe_info = xshe.suspend(query_date)
    xshg_info = xshg.suspend(query_date)

    return pd.concat([xshe_info, xshg_info]).reset_index(drop=True)[['停(复)牌时间', '证券代码', '证券简称', '状态', '原因', '期限']]


def exchange_suspend_info(ref_date, force_update=False):
    start_date = ref_date

    if not force_update:
        start_date = (find_latest_date() + dt.timedelta(days=1))

    end_date = ref_date
    date_range = pd.date_range(start_date, end_date)

    datas = []
    for date in date_range:
        if isBizDay('china.sse', date):
            datas.append(suspend_info(date.strftime('%Y-%m-%d')))
            spyder_logger.info('Scraping finished for date {0}'.format(date))

    if not datas:
        spyder_logger.info('No data is available for {0}'.format(ref_date))
        return
        
    total_table = pd.concat(datas)
    total_table.drop_duplicates(['停(复)牌时间', '证券代码'], inplace=True)

    if not total_table.empty:
        insert_table(total_table,
                     ['effectiveDate',
                      'instrumentID',
                      'instrumentName',
                      'status',
                      'reason',
                      'stopTime'],
                     'suspend_info',
                     exchange_db_settings)


if __name__ == "__main__":
    exchange_suspend_info(dt.datetime(2017, 4, 17))
