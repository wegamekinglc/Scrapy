# -*- coding: utf-8 -*-
u"""
Created on 2017-4-10

@author: cheng.li
"""

import datetime as dt
import pandas as pd
from bs4 import BeautifulSoup

from PyFin.DateUtilities import Date
from PySpyder.utilities import create_engine
from PySpyder.utilities import insert_table
from PySpyder.howbuy.utilities import login
from PySpyder.howbuy.utilities import parse_table
from PySpyder.utilities import spyder_logger
from PySpyder.utilities import hedge_fund_db_settings


month_ends = ['03-31', '06-30', '09-30', '12-31']


def date_stamps(start_date, end_date):
    start_date = Date.fromDateTime(start_date)
    end_date = Date.fromDateTime(end_date)

    start_year = start_date.year()
    start_month = start_date.month()

    if start_month <= 3:
        start_month = 3
    elif start_month <= 6:
        start_month = 6
    elif start_month <= 9:
        start_month = 9
    else:
        start_month = 12

    stamps = []

    start_point = Date(start_year, start_month, 1)
    start_point = Date.endOfMonth(start_point)

    while start_point <= end_date:
        stamps.append(start_point.toDateTime().strftime('%Y%m%d'))
        start_point = Date.endOfMonth(start_point + '3m')

    return stamps


def find_latest_date():
    engine = create_engine(hedge_fund_db_settings)
    sql = 'select publicationDate from HOWBUY_FUND_HOLDING'
    exist_data = pd.read_sql(sql, engine).sort_values('publicationDate')
    if len(exist_data) > 0:
        return exist_data.iloc[len(exist_data) - 1]['publicationDate']
    else:
        return pd.Timestamp('2014-01-01')


def load_fund_holding(start_date, end_date):
    session = login()
    querl_url_template = 'http://simudata.howbuy.com/profile/favouriteStocks.htm?' \
                         'jjdm5=&zqdm=&endDate={0}&orderBy=cgsl&orderRule=Desc&page={1}'

    stamps = date_stamps(start_date, end_date)

    datas = []

    for end_date in stamps:

        page = 0
        previous_page = None

        while True:
            page += 1
            query_url = querl_url_template.format(end_date, page)

            info_data = session.post(query_url)
            soup = BeautifulSoup(info_data.text, 'lxml')

            error_message = soup.find('div', attrs={'class': 'iocn'})
            if error_message:
                raise ValueError(error_message.text)

            tables = soup.find_all('table')

            if soup == previous_page:
                break

            if tables:
                target_table = tables[1]

                if target_table.tbody.td.text == '未查询到相关数据！':
                    break

                fund_data = parse_table(target_table)
                datas.append(fund_data)
            previous_page = soup
            spyder_logger.info("Page No. {0:4d} is finished.".format(page))

        spyder_logger.info('Publication Date : {0} is finished for fund holding'.format(end_date))

    if datas:
        total_table = pd.concat(datas)
        total_table.drop_duplicates(['基金代码', '基金简称', '股票代码'], inplace=True)
        return total_table[['基金代码', '基金简称', '截止日期', '持股数量(万股)', '持股比例(%)', '变动数量(万股)', '股票代码', '股票简称']]
    else:
        spyder_logger.warning("No any data got between {0} and {1}".format(start_date, end_date))
        return pd.DataFrame()


def fund_holding_spyder(ref_date, force_update=False):
    start_date = ref_date

    if not force_update:
        start_date = (find_latest_date() + dt.timedelta(days=1))

    end_date = dt.datetime.now() - dt.timedelta(days=60)
    total_table = load_fund_holding(start_date, end_date)

    if not total_table.empty:
        insert_table(total_table,
                     ['howbuyCODE', 'fundName', 'publicationDate', 'holdingAmount', 'holdingPercentage', 'changeAmount',
                      'instrumentID',
                      'instrumentName'],
                     'howbuy_fund_holding',
                     hedge_fund_db_settings)


if __name__ == '__main__':
    fund_holding_spyder(dt.datetime(2017, 5, 5))
