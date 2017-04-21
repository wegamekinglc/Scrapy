# -*- coding: utf-8 -*-
u"""
Created on 2016-10-25

@author: cheng.li
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
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
                        if cells[4].text == '取消停牌':
                            status.append('复牌')
                            stop_times.append('')
                        else:
                            status.append('停牌')
                            stop_times.append(cells[4].text)

                        reasons.append(cells[5].text)
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


if __name__ == '__main__':
    df = suspend('2017-04-21')
    print(df)
