# -*- coding: utf-8 -*-
u"""
Created on 2016-4-13

@author: cheng.li
"""

import datetime as dt
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator


start_date = dt.datetime(2017, 1, 1)
dag_name = 'remote_windows_call'

default_args = {
    'owner': 'wegamekinglc',
    'depends_on_past': True,
    'start_date': start_date
}


dag = DAG(
    dag_id=dag_name,
    default_args=default_args,
    schedule_interval='*/5 * * * *')

task = BashOperator(task_id='simple_bash',
                    bash_command='ssh username@hostname "./remote_windows_test.bat"',
                    dag=dag)
