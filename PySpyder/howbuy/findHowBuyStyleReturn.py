# -*- coding: utf-8 -*-
u"""
Created on 2016-2-26

@author: cheng.li
"""

import datetime as dt
import pandas as pd
from bs4 import BeautifulSoup

from PySpyder.utilities import create_engine
from PySpyder.utilities import insert_table
from PySpyder.howbuy.utilities import login
from PySpyder.howbuy.utilities import parse_table
from PySpyder.utilities import spyder_logger
from PySpyder.utilities import hedge_fund_db_settings


def load_howbuy_style_return(start_month=200001, end_month=202101):
    session = login()
    querl_url_template = 'http://simudata.howbuy.com/profile/strategies.htm?staDate={0}' \
                         '&cllx=qb&endDate={1}&page={2}&syl=j1y'

    datas = []
    page = 0
    previous_page = None

    while True:
        page += 1
        query_url = querl_url_template.format(start_month, end_month, page)
        info_data = session.post(query_url)
        soup = BeautifulSoup(info_data.text, 'lxml')

        error_message = soup.find('div', attrs={'class': 'iocn'}).text
        if error_message.startswith('对不起，系统繁忙，请稍后再试'):
            raise ValueError(error_message)

        tables = soup.find_all('table')
        target_table = tables[1]

        if soup == previous_page or target_table.tbody.td.text == '未查询到相关数据！':
            break

        previous_page = soup

        fund_data = parse_table(target_table,
                                col_level=2,
                                col_names=['No.', '统计月份', '好买策略', '最大值', '最小值', '中位数', '均值', '沪深300同期收益率'])
        datas.append(fund_data)
        spyder_logger.info("Page No. {0:4d} is finished.".format(page))

    if datas:
        total_table = pd.concat(datas)
        total_table.drop_duplicates(['统计月份', '好买策略'], inplace=True)
        return total_table
    else:
        return pd.DataFrame()


def format_table(table):
    table['统计月份'] = table['统计月份'].apply(
        lambda x: str(int(x) // 100) + '-' + '{0:02d}'.format(int(x) - int(x) // 100 * 100) + '-01')
    table = table[['统计月份', '好买策略', '最大值', '最小值', '中位数', '均值']]
    table.loc[:, '最大值'] *= 100
    table.loc[:, '最小值'] *= 100
    table.loc[:, '中位数'] *= 100
    table.loc[:, '均值'] *= 100
    return table


def find_latest_date():
    engine = create_engine(hedge_fund_db_settings)
    sql = 'select tradingDate, howbuyStrategy from HOWBUY_STYLE_RET'
    exist_data = pd.read_sql(sql, engine).sort_values('tradingDate')
    if len(exist_data) > 0:
        return exist_data.iloc[len(exist_data) - 1]['tradingDate']
    else:
        return pd.Timestamp('1990-01-01')


def fund_style_return_spyder(ref_date, force_update=False):
    start_month = int(ref_date.strftime('%Y%m'))

    if not force_update:
        latest_date = find_latest_date()
        next_month_start = latest_date + dt.timedelta(days=31)
        latest_next_month = int(next_month_start.strftime('%Y%m'))
        start_month = latest_next_month

    total_table = load_howbuy_style_return(start_month)
    if not total_table.empty:
        total_table = format_table(total_table)
        insert_table(total_table,
                     ['tradingDate', 'howbuyStrategy', 'max_ret', 'min_ret', 'median_ret', 'mean_ret'],
                     'HOWBUY_STYLE_RET',
                     hedge_fund_db_settings)


if __name__ == "__main__":
    fund_style_return_spyder(dt.datetime.now())
