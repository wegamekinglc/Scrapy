# -*- coding: utf-8 -*-
u"""
Created on 2016-10-25

@author: cheng.li
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd


def suspend(query_date):
    codes = []
    names = []
    status = []
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
                    if cells[4].text == '取消停牌':
                        status.append('复牌')
                    else:
                        status.append('停牌')
                    reasons.append(cells[5].text)
            else:
                break
            page += 1

    df = pd.DataFrame({'证券代码': codes,
                       '证券简称': names,
                       '状态': status,
                       '原因': reasons})
    return df

if __name__ == '__main__':

    df = suspend('2016-10-25')
    print(df)



