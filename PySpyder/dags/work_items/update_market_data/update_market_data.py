# -*- coding: utf-8 -*-
u"""
Created on 2016-4-14

@author: cheng.li
"""

import datetime as dt
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator

start_date = dt.datetime(2017, 1, 1)
dag_name = 'update_market_data'

default_args = {
    'owner': 'wegamekinglc',
    'depends_on_past': True,
    'start_date': start_date
}


dag = DAG(
    dag_id=dag_name,
    default_args=default_args,
    schedule_interval='0 17 * * 1,2,3,4,5')

bash_command = 'ssh wegamekinglc@10.63.6.149 "./update_index_and_fund.bat {{ next_execution_date.strftime(\'%Y-%m-%d\') }}"'

task1 = BashOperator(task_id='update_index_and_fund',
                     bash_command=bash_command,
                     dag=dag)

bash_command = 'ssh wegamekinglc@10.63.6.149 "./update_future.bat {{ next_execution_date.strftime(\'%Y-%m-%d\') }}"'

task2 = BashOperator(task_id='update_future',
                     bash_command=bash_command,
                     dag=dag)

task2.set_upstream(task1)

bash_command = 'ssh wegamekinglc@10.63.6.149 "./update_equity.bat {{ next_execution_date.strftime(\'%Y-%m-%d\') }}"'

task3 = BashOperator(task_id='update_equity',
                     bash_command=bash_command,
                     dag=dag)
