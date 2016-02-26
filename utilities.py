# -*- coding: utf-8 -*-
u"""
Created on 2016-2-26

@author: cheng.li
"""

import requests
import sqlite3
import pandas as pd


DB_SETTINGS = \
    {
        "hedge_funds":
            {
                'file': 'D:/dev/laiyuchen/FOF/FOF/hedge_funds.db'
            }
    }


def create_engine(db_name):
    global DB_SETTINGS
    setting = DB_SETTINGS[db_name]
    return sqlite3.connect(setting['file'])


def insert_table(data, field_names, table_name, db_name):
    engine = create_engine(db_name)
    engine.isolation_level = "DEFERRED"
    fields = ','.join(field_names)
    l = len(field_names)
    sql = "INSERT INTO {0:s} ({1:s}) VALUES ({2:s})" \
        .format(table_name, fields, ('?,' * l)[:-1])

    engine.executemany(sql, data.values)
    engine.commit()
    engine.close()


def try_parse_percentage(x):
    u"""

    parse字符串形式的百分数为浮点数

    :param x: 百分数
    :return: float
    """

    if not x.endswith('%') and x != '--':
        return x

    try:
        return float(x[:-1]) / 100.
    except ValueError:
        return 0.


def try_parse_float(x):

    try:
        return float(x)
    except ValueError:
        return x


def parse_table(target_table, col_level=1, col_names=None):

    full_table = []
    trs = target_table.find_all('tr')
    if not col_names:
        headers = trs[0]
        cols = headers.find_all('td')
        col_names = [col.get_text().strip() for col in cols]

    for tr in trs[col_level:]:
        tds = tr.find_all('td')
        full_table.append({})
        for i, col in enumerate(col_names):
            value = tds[i].get_text().strip()
            value = try_parse_percentage(value)
            value = try_parse_float(value)
            full_table[-1][col] = value

    return pd.DataFrame(full_table)


def login():
    session = requests.Session()
    session.headers['Content-Type'] = 'Content-Type:application/x-www-form-urlencoded'
    login_url = 'http://simudata.howbuy.com/login.htm?name=howbuyi&psw=808080&提交=登陆'
    session.post(login_url)
    return session
