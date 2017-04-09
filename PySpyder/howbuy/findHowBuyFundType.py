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


def find_latest_date():
    engine = create_engine()
    sql = 'select DISTINCT(setupDate) from HOWBUY_FUND_TYPE'
    data = pd.read_sql(sql, engine).sort_values('setupDate')
    if len(data) > 0:
        return data.iloc[-1]['setupDate']
    else:
        return pd.Timestamp('1990-01-01')


def load_howbuy_fund_type(latest_date='1900-01-01'):
    session = login()
    quert_url_template = 'http://simudata.howbuy.com/profile/newJjjz.htm?orderBy=clrq' \
                         '&orderByDesc=true&jjdm=&jldm=&glrm=&cllx=qb&zzxs=qb&syMin=&syMax=&' \
                         'page={0}&perPage=30'

    full_table = []
    page = 0
    previous_page = None

    while True:
        page += 1
        query_url = quert_url_template.format(page)
        info_data = session.post(query_url)
        soup = BeautifulSoup(info_data.text, 'lxml')

        if soup == previous_page:
            break

        previous_page = soup
        tables = soup.find_all('table')
        target_table = tables[1]

        fundData = parse_table(target_table)
        full_table.append(fundData)
        print(u"第{0:4d}页数据抓取完成".format(page))

        if pd.Timestamp(fundData.iloc[-1]['成立日期']) < latest_date:
            break

    total_table = pd.concat(full_table)
    total_table = total_table[(total_table['净值日期'] != 0) & (total_table['成立日期'] != 0)]
    total_table = total_table[pd.to_datetime(total_table['成立日期'], format='%Y-%m-%d') > latest_date]
    return total_table[['基金代码', '基金简称', '基金管理人', '基金经理', '成立日期', '好买策略', '复权单位净值', '净值日期']]


if __name__ == "__main__":
    latest_date = find_latest_date()
    total_table = load_howbuy_fund_type(latest_date)
    insert_table(total_table,
                 ['howbuyCODE', 'fundName', 'fundManagementComp', 'manager', 'setupDate', 'howbuyStrategy', 'adjPrice', 'priceDate'],
                 'HOWBUY_FUND_TYPE')