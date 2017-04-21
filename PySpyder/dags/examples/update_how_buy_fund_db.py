# -*- coding: utf-8 -*-
u"""
Created on 2016-4-9

@author: cheng.li
"""

import sys
import datetime as dt
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG

sys.path.append('/path/toyour/scrapy/root')

from PySpyder.howbuy.findHowBuyFundIndex import fund_index_spyder
from PySpyder.howbuy.findHowBuyFundType import fund_type_spyder
from PySpyder.howbuy.findHowBuyStyleReturn import fund_style_return_spyder
from PySpyder.howbuy.findHowBuyFundHolding import fund_holding_spyder

start_date = dt.datetime(2017, 1, 1)
dag_name = 'update_how_buy_fund_db'

default_args = {
    'owner': 'wegamekinglc',
    'depends_on_past': True,
    'start_date': start_date
}

dag = DAG(
    dag_id=dag_name,
    default_args=default_args,
    schedule_interval='30 9 * * 1,2,3,4,5')


def update_fund_index(ds, **kwargs):
    ref_date = kwargs['next_execution_date']
    fund_index_spyder(ref_date=ref_date)
    return 'updating for fund index is finished for {0}'.format(ref_date)


def update_fund_type(ds, **kwargs):
    ref_date = kwargs['next_execution_date']
    fund_type_spyder(ref_date=ref_date)
    return 'updating for fund type is finished for {0}'.format(ref_date)


def update_fund_style_ret(ds, **kwargs):
    ref_date = kwargs['next_execution_date']
    fund_style_return_spyder(ref_date=ref_date)
    return 'updating for fund style return is finished for {0}'.format(ref_date)


def update_fund_holding(ds, **kwargs):
    ref_date = kwargs['next_execution_date']
    fund_holding_spyder(ref_date=ref_date)
    return 'updating for fund holding is finished for {0}'.format(ref_date)


run_this1 = PythonOperator(
    task_id='update_fund_index',
    provide_context=True,
    python_callable=update_fund_index,
    dag=dag)

run_this2 = PythonOperator(
    task_id='update_fund_type',
    provide_context=True,
    python_callable=update_fund_type,
    dag=dag)

run_this3 = PythonOperator(
    task_id='update_fund_style_return',
    provide_context=True,
    python_callable=update_fund_style_ret,
    dag=dag)

run_this4 = PythonOperator(
    task_id='update_fund_holding',
    provide_context=True,
    python_callable=update_fund_holding,
    dag=dag)
