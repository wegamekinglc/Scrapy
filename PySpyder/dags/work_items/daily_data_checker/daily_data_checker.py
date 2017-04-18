# -*- coding: utf-8 -*-

import datetime as dt
from airflow.operators.bash_operator import BashOperator
from airflow.models import DAG


args = {
    'owner': 'wegamekinglc',
    'start_date': dt.datetime(2017, 4, 13)
}


dag = DAG(dag_id='daily_data_checke',
          default_args=args,
          schedule_interval='45 17 * * 1,2,3,4,5')


task1 = BashOperator(task_id='run_check',
                     bash_command='python path/to/datachecker/CheckerController.py {{ next_execution_date.strftime(\'%Y-%m-%d\') }}',
                     dag=dag)
