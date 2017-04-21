# -*- coding: utf-8 -*-
u"""
Created on 2016-10-25

@author: cheng.li
"""

import json
import requests
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

    with requests.Session() as session:
        session.headers['Referer'] = 'http://www.sse.com.cn/disclosure/dealinstruc/suspension/'

        template_url = 'http://query.sse.com.cn/' \
                       'infodisplay/querySpecialTipsInfoByPage.do?' \
                       'jsonCallBack=jsonpCallback45028&isPagination=true&searchDate={query_date}' \
                       '&bgFlag=1&searchDo=1&pageHelp.pageSize=5000&pageHelp.pageNo=1' \
                       '&pageHelp.beginPage=1&pageHelp.cacheSize=1&_=1477364635046'

        query_url = template_url.format(query_date=query_date)

        info_data = session.post(query_url)

        info_data.encoding = 'utf8'
        soup = BeautifulSoup(info_data.text, 'lxml')
        content = json.loads(soup.text.split('(')[1].strip(')'))

        json_data = content['result']

        for row in json_data:
            if row['showDate'] == query_date and row['productCode'].startswith('6'):
                codes.append(row['productCode'])
                names.append(row['productName'])

                if row['stopTime'].find('停牌终止') != -1:
                    status.append('复牌')
                else:
                    status.append('停牌')
                stop_times.append(row['stopTime'])
                reasons.append(row['stopReason'])

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
    sql = "select url from announcement_info where reportDate='{0}'".format(query_date)
    exist_data = pd.read_sql(sql, engine)
    return exist_data


def announcement(query_date):
    codes = []
    titles = []
    urls = []
    report_dates = []

    with requests.Session() as session:
        session.headers['Referer'] = 'http://www.sse.com.cn/disclosure/listedinfo/announcement/'

        template_url = 'http://query.sse.com.cn/' \
                       'infodisplay/queryLatestBulletinNew.do?' \
                       'jsonCallBack=jsonpCallback98209&isPagination=true&productId=&keyWord=&reportType2=&' \
                       'reportType=ALL&beginDate={query_date}&endDate={query_date}&pageHelp.pageSize=5000&' \
                       'pageHelp.pageCount=50&pageHelp.pageNo=1&pageHelp.beginPage=1&' \
                       'pageHelp.cacheSize=1&pageHelp.endPage=5&_=1492758467504'

        query_url = template_url.format(query_date=query_date)

        info_data = session.post(query_url)

        info_data.encoding = 'utf8'
        soup = BeautifulSoup(info_data.text, 'lxml')
        content = json.loads(soup.text.split('(')[1].strip(')'))

        json_data = content['result']

        for row in json_data:
            codes.append(row['security_Code'])
            titles.append(row['title'])
            urls.append(row['URL'])
            report_dates.append(row['SSEDate'])

    df = pd.DataFrame({'报告日期': report_dates,
                       '证券代码': codes,
                       '标题': titles,
                       'url': urls})

    if df.empty:
        spyder_logger.warning('No data found for the date {0}'.format(query_date))

    exist_data = find_existing(query_date)
    new_records = set(df.url).difference(set(exist_data.url))
    df = df[df.url.isin(new_records)]

    return df


if __name__ == '__main__':
    # sse
    df = announcement('2017-04-17')
    print(df)
