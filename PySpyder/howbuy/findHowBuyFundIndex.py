# -*- coding: utf-8 -*-
u"""
Created on 2016-2-26

@author: cheng.li
"""

from __future__ import division

import pandas as pd
from bs4 import BeautifulSoup

from PySpyder.howbuy.utilities import create_engine
from PySpyder.howbuy.utilities import insert_table
from PySpyder.howbuy.utilities import login
from PySpyder.howbuy.utilities import parse_table


def load_fund_index(startMonth='2006-01', endMonth='2019-01'):
    startMonth = startMonth.replace('-', '')
    endMonth = endMonth.replace('-', '')

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
            query_url = querl_url_template.format(startMonth, endMonth, page, code)
            info_data = session.post(query_url)
            soup = BeautifulSoup(info_data.text, 'lxml')

            if soup == previous_page:
                datas = datas[:-1]
                break

            previous_page = soup
            tables = soup.find_all('table')
            target_table = tables[1]

            fundData = parse_table(target_table)
            datas.append(fundData)

        print('基金指数：{0}抓取完成'.format(code))

    total_table = pd.concat(datas)
    return total_table.reset_index(drop=True)


def format_table(table):
    table['统计月份'] = table['统计月份'].apply(lambda x: str(int(x) // 100) + '-' + '{0:02d}'.format(int(x) - int(x) // 100 * 100) + '-01')
    table = table[['统计月份', '指数代码', '指数简称', '指数点位', '涨跌幅', '调整后沪深300']]
    return table


def filter_data(table):
    engine = create_engine()
    sql = 'select tradingDate, howbuyCode from HOWBUY_FUND_INDEX'
    exist_data = pd.read_sql(sql, engine).sort_values('tradingDate')
    last_update_dates = exist_data.groupby('howbuyCode').last()

    for code in last_update_dates.index:
        last_date = last_update_dates.ix[code]['tradingDate']
        table = table[(table['指数代码'] != code) | (pd.to_datetime(table['统计月份']) > last_date)]

    return table


if __name__ == '__main__':
    total_table = load_fund_index(startMonth='2001-06')
    total_table = format_table(total_table)
    total_table = filter_data(total_table)
    insert_table(total_table,
                 ['tradingDate', 'howbuyCode', 'indexName', 'indexLevel', 'indexLevelChg', 'adjustedHS300'],
                 'HOWBUY_FUND_INDEX')
