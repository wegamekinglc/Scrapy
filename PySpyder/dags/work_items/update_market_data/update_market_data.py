# -*- coding: utf-8 -*-
u"""
Created on 2016-4-14

@author: cheng.li
"""

import datetime as dt
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator

start_date = dt.datetime(2017, 4, 1)
dag_name = 'update_market_data'

default_args = {
    'owner': 'wegamekinglc',
    'depends_on_past': True,
    'start_date': start_date
}


dag = DAG(
    dag_id=dag_name,
    default_args=default_args,
    schedule_interval='0 16 * * *')

task = BashOperator(task_id='update_index_and_fund',
                    bash_command='ssh yourusername@hostname "./update_index_and_fund.bat {{ ds }}"',
                    dag=dag)
