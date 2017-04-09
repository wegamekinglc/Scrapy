# -*- coding: utf-8 -*-
u"""
Created on 2016-2-26

@author: cheng.li
"""

import pandas as pd
from bs4 import BeautifulSoup

from PySpyder.howbuy.utilities import create_engine
from PySpyder.howbuy.utilities import insert_table
from PySpyder.howbuy.utilities import login
from PySpyder.howbuy.utilities import parse_table


def load_howbuy_style_return(startMonth='2000-01', endMonth='2019-01'):
    startMonth = startMonth.replace('-', '')
    endMonth = endMonth.replace('-', '')

    session = login()
    querl_url_template = 'http://simudata.howbuy.com/profile/strategies.htm?staDate={0}' \
                         '&cllx=qb&endDate={1}&page={2}&syl=j1y'

    datas = []
    page = 0
    previous_page = None

    while True:
        page += 1
        query_url = querl_url_template.format(startMonth, endMonth, page)
        info_data = session.post(query_url)
        soup = BeautifulSoup(info_data.text, 'lxml')

        if soup == previous_page:
            break

        previous_page = soup
        tables = soup.find_all('table')
        target_table = tables[1]

        fundData = parse_table(target_table,
                               col_level=2,
                               col_names=['No.', '统计月份', '好买策略', '最大值', '最小值', '中位数', '均值', '沪深300同期收益率'])
        datas.append(fundData)
        print(u"第{0:4d}页数据抓取完成".format(page))

    total_table = pd.concat(datas)
    return total_table.reset_index(drop=True)


def format_table(table):
    table['统计月份'] = table['统计月份'].apply(lambda x: str(int(x) // 100) + '-' + '{0:02d}'.format(int(x) - int(x) // 100 * 100) + '-01')
    table = table[['统计月份', '好买策略', '最大值', '最小值', '中位数', '均值']]
    table.loc[:, '最大值'] *= 100
    table.loc[:, '最小值'] *= 100
    table.loc[:, '中位数'] *= 100
    table.loc[:, '均值'] *= 100
    return table


def filter_data(table):
    engine = create_engine()
    sql = 'select tradingDate, howbuyStrategy from HOWBUY_STYLE_RET'
    exist_data = pd.read_sql(sql, engine).sort_values('tradingDate')
    last_update_dates = exist_data.groupby('howbuyStrategy').last()

    for name in last_update_dates.index:
        last_date = last_update_dates.ix[name]['tradingDate']
        table = table[(table['好买策略'] != name) | (pd.to_datetime(table['统计月份']) > last_date)]

    return table


if __name__ == "__main__":
    total_table = load_howbuy_style_return()
    total_table = format_table(total_table)
    total_table = filter_data(total_table)
    insert_table(total_table,
                 ['tradingDate', 'howbuyStrategy', 'max_ret', 'min_ret', 'median_ret', 'mean_ret'],
                 'HOWBUY_STYLE_RET')