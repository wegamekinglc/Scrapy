# -*- coding: utf-8 -*-

import json
import requests
from bs4 import BeautifulSoup
import pandas as pd


def suspend(query_date):
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

        suspend_table = pd.DataFrame(content['result'])
        del suspend_table['ROWNUM_']
        del suspend_table['seq']
        suspend_table = suspend_table[suspend_table.productCode.str.startswith(('0', '3', '6'))]
        suspend_table.reset_index(drop=True, inplace=True)
    return suspend_table


if __name__ == '__main__':

    # sse
    df = suspend('2016-10-11')
    print(df)
