# -*- coding: utf-8 -*-
u"""
Created on 2016-10-24

@author: cheng.li
"""

from bs4 import BeautifulSoup
from selenium import webdriver

scrapy_groups = ['交易提示', '上交所公告']


def extract_sub_links(page_item, iter):
    sub_links = page_item.find_all('li', {'class': 'menu_lv_' + str(iter)})
    if sub_links:
        for l in sub_links:
            yield from extract_sub_links(l, iter+1)
    else:
        yield page_item

if __name__ == '__main__':

    driver = webdriver.PhantomJS('D:/dev/phantomjs-2.1.1-windows/bin/phantomjs.exe')
    home_page = 'http://www.sse.com.cn'
    driver.get('http://www.sse.com.cn/disclosure/overview/')
    js = 'document.write(sseMenuObj.initLeftMenu())'
    driver.execute_script(js)

    soup = BeautifulSoup(driver.page_source, 'lxml')

    links = []

    for item in soup.find_all('li', {'class': 'menu_lv_1'}):
        sub_group_name = item.find('span', {'class': 'ib_mid'}).text
        if sub_group_name in scrapy_groups:
            res = item
            for page in extract_sub_links(res, 2):
                link = list(page)[0]['href']
                links.append(home_page + link)

    driver.get(links[0])

    soup2 = BeautifulSoup(driver.page_source, 'lxml')
    print(soup2)
