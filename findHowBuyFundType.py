# -*- coding: utf-8 -*-
u"""
Created on 2016-2-26

@author: cheng.li
"""

from bs4 import BeautifulSoup
import pandas as pd
from utilities import parse_table
from utilities import login
from utilities import create_engine
from utilities import insert_table


def find_latest_date():
    engine = create_engine('hedge_funds')
    sql = 'select DISTINCT(setupDate) from HOWBUY_FUND_TYPE'
    data = pd.read_sql(sql, engine).sort_values('setupDate')
    return data.iloc[-1]['setupDate']


def scrapy_howbuy_fund_type(latest_date='1900-01-01'):
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

        if fundData.iloc[-1]['成立日期'] < latest_date:
            break

    total_table = pd.concat(full_table)
    total_table = total_table[total_table['成立日期'] > latest_date]
    return total_table[['基金代码', '基金简称', '基金管理人', '基金经理', '成立日期', '好买策略']]


if __name__ == "__main__":
    latest_date = find_latest_date()
    total_table = scrapy_howbuy_fund_type(latest_date)
    insert_table(total_table,
                 ['howbuyCODE', 'name', 'fundManagementComp', 'manager', 'setupDate', 'howbuyStrategy'],
                 'HOWBUY_FUND_TYPE',
                 'hedge_funds')