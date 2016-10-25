# -*- coding: utf-8 -*-
u"""
Created on 2016-10-25

@author: cheng.li
"""

import json
import requests
from bs4 import BeautifulSoup
import pandas as pd


def suspend(query_date):
    codes = []
    names = []
    status = []
    reasons = []

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

                reasons.append(row['stopReason'])

    df = pd.DataFrame({'证券代码': codes,
                       '证券简称': names,
                       '状态': status,
                       '原因': reasons})
    return df


if __name__ == '__main__':

    # sse
    df = suspend('2016-10-11')
    print(df)
