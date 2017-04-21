# -*- coding: utf-8 -*-
u"""
Created on 2016-2-26

@author: cheng.li
"""

from __future__ import division

import datetime as dt
import pandas as pd
from bs4 import BeautifulSoup

from PySpyder.utilities import create_engine
from PySpyder.utilities import insert_table
from PySpyder.howbuy.utilities import login
from PySpyder.howbuy.utilities import parse_table
from PySpyder.utilities import spyder_logger
from PySpyder.utilities import hedge_fund_db_settings


def load_fund_index(start_month=200601, end_month=202201):
    session = login()
    querl_url_template = 'http://simudata.howbuy.com/profile/howbuyIndex.htm?staDate={0}' \
                         '&orderRule=Desc&endDate={1}&page={2}&smzs={3}'

    index_codes = ['HB0001',
                   'HB0011',
                   'HB0012',
                   'HB0014',
                   'HB0015',
                   'HB0016',
                   'HB0017',
                   'HB0018',
                   'HB001b',
                   'HB001d']

    datas = []

    for code in index_codes:
        page = 0
        previous_page = None

        while True:
            page += 1
            query_url = querl_url_template.format(start_month, end_month, page, code)
            info_data = session.post(query_url)
            soup = BeautifulSoup(info_data.text, 'lxml')

            error_message = soup.find('div', attrs={'class': 'iocn'}).text
            if error_message.startswith('对不起，系统繁忙，请稍后再试'):
                raise ValueError(error_message)

            tables = soup.find_all('table')
            target_table = tables[1]

            if soup == previous_page or target_table.tbody.td.text == '未查询到相关数据！':
                break

            fund_data = parse_table(target_table)
            datas.append(fund_data)
            previous_page = soup

        spyder_logger.info('Fund index：{0} is finished.'.format(code))

    if datas:
        total_table = pd.concat(datas)
        total_table.drop_duplicates(['统计月份', '指数代码'], inplace=True)
        return total_table.reset_index(drop=True)
    else:
        return pd.DataFrame()


def format_table(table):
    table['统计月份'] = table['统计月份'].apply(
        lambda x: str(int(x) // 100) + '-' + '{0:02d}'.format(int(x) - int(x) // 100 * 100) + '-01')
    table = table[['统计月份', '指数代码', '指数简称', '指数点位', '涨跌幅', '调整后沪深300']]
    return table


def find_latest_date():
    engine = create_engine(hedge_fund_db_settings)
    sql = 'select tradingDate, howbuyCode from HOWBUY_FUND_INDEX'
    exist_data = pd.read_sql(sql, engine).sort_values('tradingDate')
    if len(exist_data) > 0:
        return exist_data.iloc[len(exist_data) - 1]['tradingDate']
    else:
        return pd.Timestamp('1990-01-01')


def fund_index_spyder(ref_date, force_update=False):
    start_month = int(ref_date.strftime('%Y%m'))

    if not force_update:
        latest_date = find_latest_date()
        latest_next_month = int(latest_date.strftime('%Y%m')) + 1
        start_month = latest_next_month

    total_table = load_fund_index(start_month=start_month)

    if not total_table.empty:
        total_table = format_table(total_table)
        insert_table(total_table,
                     ['tradingDate', 'howbuyCode', 'indexName', 'indexLevel', 'indexLevelChg', 'adjustedHS300'],
                     'HOWBUY_FUND_INDEX',
                     hedge_fund_db_settings)


if __name__ == '__main__':
    fund_index_spyder(dt.datetime.now(), force_update=False)
