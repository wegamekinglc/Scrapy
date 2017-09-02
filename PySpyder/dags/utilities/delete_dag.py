# -*- coding: utf-8 -*-
u"""
Created on 2016-4-15

@author: cheng.li
"""

import sys
import psycopg2

DB_SETTINGS = {'hostname': '192.168.0.101',
               'user': 'postgres',
               'passwd': 'we083826',
               'db': 'airflow'}

QUERIES = {"delete from xcom where dag_id = '{dag_id}'",
           "delete from task_fail where dag_id = '{dag_id}'",
           "delete from task_instance where dag_id = '{dag_id}'",
           "delete from sla_miss where dag_id = '{dag_id}'",
           "delete from log where dag_id = '{dag_id}'",
           "delete from job where dag_id = '{dag_id}'",
           "delete from dag_run where dag_id = '{dag_id}'",
           "delete from dag where dag_id = '{dag_id}'",
           "delete from dag_stats where dag_id = '{dag_id}'",
           "delete from task_fail where dag_id = '{dag_id}'",
           "delete from task_instance where dag_id = '{dag_id}'"}


def delete_dag(dag_id):
    """
    Delete specific dag from the airflow db
    """
    with psycopg2.connect(host=DB_SETTINGS['hostname'],
                          user=DB_SETTINGS['user'],
                          password=DB_SETTINGS['passwd'],
                          database=DB_SETTINGS['db']) as conn:
        cur = conn.cursor()
        for query in QUERIES:
            print(query.format(dag_id=dag_id))
            cur.execute(query.format(dag_id=dag_id))


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Please enter dag id to delete')
        sys.exit(-1)
    else:
        delete_dag(sys.argv[1])
