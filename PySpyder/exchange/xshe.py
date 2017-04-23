# -*- coding: utf-8 -*-
u"""
Created on 2016-10-25

@author: cheng.li
"""


import os
import time
import datetime as dt
import requests
from requests.exceptions import ConnectionError
from bs4 import BeautifulSoup
import pandas as pd
from PySpyder.utilities import exchange_db_settings
from PySpyder.utilities import create_engine
from PySpyder.utilities import spyder_logger


def suspend(query_date):
    codes = []
    names = []
    status = []
    reasons = []
    stop_times = []

    previous_page = None

    with requests.Session() as session:
        session.headers['Referer'] = 'https://www.szse.cn/main/disclosure/news/tfpts/'
        session.headers['Host'] = 'www.szse.cn'
        session.headers['Origin'] = 'https://www.szse.cn'
        query_url = 'https://www.szse.cn/szseWeb/FrontController.szse'

        page = 1

        while True:
            info_data = session.post(query_url, data={'ACTIONID': 7,
                                                      'AJAX': 'AJAX-TRUE',
                                                      'CATALOGID': 1798,
                                                      'TABKEY': 'tab1',
                                                      'REPORT_ACTION': 'navigate',
                                                      'txtKsrq': query_date,
                                                      'txtZzrq': query_date,
                                                      'tab1PAGECOUNT': 999,
                                                      'tab1RECORDCOUNT': 999999,
                                                      'tab1PAGENUM': page})
            info_data.encoding = 'gbk'
            soup = BeautifulSoup(info_data.text, 'lxml')

            if soup == previous_page:
                break

            table = soup.find_all(attrs={'class': 'cls-data-table-common cls-data-table'})[0]
            rows = table.find_all('tr')
            if rows:
                for row in rows:
                    cells = row.find_all('td')
                    if cells:
                        codes.append(cells[0].text)
                        names.append(cells[1].text)

                        info_message = cells[4].text.strip()

                        if info_message.find('取消停牌') != -1:
                            status.append('复牌')
                            stop_times.append('')
                        else:
                            status.append('停牌')
                            stop_times.append(info_message)

                        reasons.append(cells[5].text.strip())
            else:
                break
            page += 1
            previous_page = soup

    df = pd.DataFrame({'停(复)牌时间': query_date,
                       '证券代码': codes,
                       '证券简称': names,
                       '状态': status,
                       '原因': reasons,
                       '期限': stop_times})

    if df.empty:
        spyder_logger.warning('No data found for the date {0}'.format(query_date))
    return df


def find_existing(query_date):
    engine = create_engine(exchange_db_settings)
    sql = "select url from announcement_info where reportDate ='{0}' and exchangePlace = 'xshe'".format(query_date)
    exist_data = pd.read_sql(sql, engine)
    return exist_data


def match_codes(short_names, full_codes):
    codes = []
    for name in short_names:
        try:
            codes.append(full_codes[full_codes['A股简称'] == name]['A股代码'].iloc[0])
        except IndexError:
            codes.append(0)
    return codes


def announcement(query_date):

    full_codes_path = os.path.join(os.path.dirname(__file__), 'data/xshe.xlsx')
    full_codes = pd.read_excel(full_codes_path)[['A股代码', 'A股简称']]

    with requests.Session() as session:
        session.headers['Referer'] = 'http://www.sse.com.cn/disclosure/listedinfo/announcement/'

        query_url = 'http://disclosure.szse.cn/m/search0425.jsp'

        page = 1
        previous_page = None

        datas = []
        exist_data = find_existing(query_date)

        while True:

            short_names = []
            titles = []
            urls = []
            report_dates = []

            tries = 0

            while True:
                try:
                    info_data = session.post(query_url, data={'startTime': query_date,
                                                              'endTime': query_date,
                                                              'pageNo': page})
                    break
                except ConnectionError:
                    tries += 1
                    if tries >= 5:
                        raise
                    time.sleep(10)
                    continue

            info_data.encoding = 'gbk'
            soup = BeautifulSoup(info_data.text, 'lxml')

            if soup == previous_page:
                break

            rows = soup.find_all('td', attrs={'class': 'td2'})

            for row in rows:
                titles.append(row.a.text)
                short_names.append(row.a.text.split('：')[0])
                urls.append('http://disclosure.szse.cn/' + row.a['href'])
                report_dates.append(row.span.text[1:-1])

            codes = match_codes(short_names, full_codes)

            previous_page = soup
            page += 1

            df = pd.DataFrame({'报告日期': report_dates,
                               '证券代码': codes,
                               '标题': titles,
                               'url': urls,
                               'updateTime': dt.datetime.now(),
                               'exchangePlace': 'xshe'})

            new_records = set(df.url).difference(set(exist_data.url))
            original_length = len(df)
            df = df[df.url.isin(new_records)]
            datas.append(df)

            if len(df) != original_length:
                break

    df = pd.concat(datas)
    df.drop_duplicates(['url'], inplace=True)

    if df.empty:
        spyder_logger.warning('No data found for the date {0}'.format(query_date))

    return df


if __name__ == '__main__':
    df = announcement('2017-04-22')
    print(df)
