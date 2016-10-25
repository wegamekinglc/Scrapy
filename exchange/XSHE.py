# -*- coding: utf-8 -*-
u"""
Created on 2016-10-25

@author: cheng.li
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd


if __name__ == '__main__':

    query_date = '2016-10-25'

    codes = []
    names = []
    stop_times = []
    back_times = []
    durations = []
    reasons = []

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

            rows = soup.find_all('tr', {'class': 'cls-data-tr'})
            if rows:
                for row in rows:
                    cells = row.find_all('td', {'class', 'cls-data-td'})
                    codes.append(cells[0].text)
                    names.append(cells[1].text)
                    stop_times.append(cells[2].text)
                    back_times.append(cells[3].text)
                    durations.append(cells[4].text)
                    reasons.append(cells[5].text)
            else:
                break
            page += 1

    df = pd.DataFrame({'证券代码': codes,
                       '证券简称': names,
                       '停牌时间': stop_times,
                       '复牌时间': back_times,
                       '停牌期限': durations,
                       '停牌原因': reasons})

    print(df)



