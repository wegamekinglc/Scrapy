# -*- coding: utf-8 -*-
u"""
Created on 2016-10-25

@author: cheng.li
"""

import pandas as pd

import PySpyder.exchange.xshe as xshe
import PySpyder.exchange.xshg as xshg


def suspend_info(query_date):
    xshe_info = xshe.suspend(query_date)
    xshg_info = xshg.suspend(query_date)

    return pd.concat([xshe_info, xshg_info]).reset_index(drop=True)


if __name__ == "__main__":
    res = suspend_info('2016-10-25')
    print(res)