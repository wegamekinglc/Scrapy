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

from PySpyder.exchange.findSuspendInfo import exchange_suspend_info
from PySpyder.exchange.findAnnouncementInfo import exchange_announcement_info

start_date = dt.datetime(2015, 1, 1)
dag_name = 'update_exchange_db'

default_args = {
    'owner': 'wegamekinglc',
    'depends_on_past': True,
    'start_date': start_date
}

dag = DAG(
    dag_id=dag_name,
    default_args=default_args,
    schedule_interval='0 9 * * *')


def update_suspend_info(ds, **kwargs):
    ref_date = kwargs['next_execution_date']
    exchange_suspend_info(ref_date=ref_date)
    return 'updating for exchange suspend info is finished for {0}'.format(ref_date)


run_this1 = PythonOperator(
    task_id='update_exchange_suspend_info',
    provide_context=True,
    python_callable=update_suspend_info,
    dag=dag)


def update_announcement_info(ds, **kwargs):
    ref_date = kwargs['next_execution_date']
    exchange_announcement_info(ref_date=ref_date)
    return 'updating for exchange announcement info is finished for {0}'.format(ref_date)


run_this2 = PythonOperator(
    task_id='update_announcement_info',
    provide_context=True,
    python_callable=update_announcement_info,
    dag=dag)

