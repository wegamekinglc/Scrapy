# -*- coding: utf-8 -*-
u"""
Created on 2016-4-14

@author: cheng.li
"""

import datetime as dt
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator

start_date = dt.datetime(2017, 4, 18)
dag_name = 'update_alpha_data'

default_args = {
    'owner': 'wegamekinglc',
    'depends_on_past': True,
    'start_date': start_date
}


dag = DAG(
    dag_id=dag_name,
    default_args=default_args,
    schedule_interval='0 19 * * 1,2,3,4,5')


bash_command = """
result=`ssh wegamekinglc@10.63.6.149 "./update_multi_factor_db.bat {{ next_execution_date.strftime(\'%Y%m%d\') }}"`
echo $result
[[ $result =~ "Multi factor db updating is finished" ]] && exit 0
exit -1
"""

task1 = BashOperator(task_id='update_multi_factor_db',
                     bash_command=bash_command,
                     dag=dag)

bash_command = """
result=`ssh wegamekinglc@10.63.6.149 "./update_non_alpha_data.bat {{ next_execution_date.strftime(\'%Y%m%d\') }}"`
echo $result
[[ $result =~ "Non alpha data updating is finished" ]] && exit 0
exit -1
"""

task2 = BashOperator(task_id='update_non_alpha_data',
                     bash_command=bash_command,
                     dag=dag)

task2.set_upstream(task1)

bash_command = """
result=`ssh wegamekinglc@10.63.6.149 "./update_non_alpha_data_500.bat {{ next_execution_date.strftime(\'%Y%m%d\') }}"`
echo $result
[[ $result =~ "Non alpha data updating is finished" ]] && exit 0
exit -1
"""

task3 = BashOperator(task_id='update_non_alpha_data_500',
                     bash_command=bash_command,
                     dag=dag)

task3.set_upstream(task2)
