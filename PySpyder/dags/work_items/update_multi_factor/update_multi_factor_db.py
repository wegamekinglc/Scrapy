# -*- coding: utf-8 -*-
"""
Created on 2017-5-9

@author: cheng.li
"""

import datetime as dt
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
import sqlalchemy
import pandas as pd
from simpleutils import CustomLogger
from PyFin.api import isBizDay


logger = CustomLogger('MULTI_FACTOR', 'info')


start_date = dt.datetime(2017, 4, 26)
dag_name = 'update_multi_factor_db'

default_args = {
    'owner': 'wegamekinglc',
    'depends_on_past': True,
    'start_date': start_date
}

dag = DAG(
    dag_id=dag_name,
    default_args=default_args,
    schedule_interval='0 18 * * 1,2,3,4,5'
)

date_formatted_tables = {'FactorIndicator_500'}


def create_ms_engine(db):
    ms_user = 'sa'
    ms_pwd = 'A12345678!'
    return sqlalchemy.create_engine(
        'mssql+pymssql://{0}:{1}@10.63.6.219/{2}?charset=cp936'.format(ms_user, ms_pwd, db))


def create_my_engine():
    my_user = 'sa'
    my_pwd = 'We051253524522'
    return sqlalchemy.create_engine(
        'mysql+pymysql://{0}:{1}@rm-bp1psdz5615icqc0yo.mysql.rds.aliyuncs.com/multifactor?charset=utf8'.format(my_user, my_pwd))


def fetch_date(table, query_date, engine):
    query_date = query_date.replace('-', '')
    if table in date_formatted_tables:
        sql = "select * from {0} where Date = '{1}'".format(table, query_date)
    else:
        sql = "select * from {0} where Date = {1}".format(table, query_date)
    df = pd.read_sql_query(sql, engine)

    if table == 'FactorData':
        cols = df.columns.tolist()
        cols[2] = '申万一级行业'
        cols[3] = '申万二级行业'
        cols[4] = '申万三级行业'
        df.columns = cols

    if table != 'FactorIndicator_500':
        df['Date'] = pd.to_datetime(df.Date.astype(str))
    return df


def delete_data(table, query_date, engine):
    sql = "delete from {0} where Date = '{1}'".format(table, query_date)
    return engine.execute(sql)


def insert_data(table, df, engine):
    return df.to_sql(table, engine, index=False, if_exists='append')


def update_factor_data(ds, **kwargs):
    ref_date = kwargs['next_execution_date']\

    if not isBizDay('china.sse', ref_date):
        logger.info("{0} is not a business day".format(ref_date))
        return 0

    ref_date = ref_date.strftime('%Y-%m-%d')

    conn1 = create_ms_engine('MultiFactor')
    df = fetch_date('FactorData', ref_date, conn1)

    conn2 = create_my_engine()

    delete_data('factor_data', ref_date, conn2)
    insert_data('factor_data', df, conn2)
    return 0


def update_index_components(ds, **kwargs):
    ref_date = kwargs['next_execution_date']\

    if not isBizDay('china.sse', ref_date):
        logger.info("{0} is not a business day".format(ref_date))
        return 0

    ref_date = ref_date.strftime('%Y-%m-%d')

    conn1 = create_ms_engine('MultiFactor')
    df = fetch_date('IndexComponents', ref_date, conn1)

    conn2 = create_my_engine()

    delete_data('index_components', ref_date, conn2)
    insert_data('index_components', df, conn2)
    return 0


def update_index_data(ds, **kwargs):
    ref_date = kwargs['next_execution_date']\

    if not isBizDay('china.sse', ref_date):
        logger.info("{0} is not a business day".format(ref_date))
        return 0

    ref_date = ref_date.strftime('%Y-%m-%d')

    conn1 = create_ms_engine('MultiFactor')
    df = fetch_date('StockIndices', ref_date, conn1)

    conn2 = create_my_engine()

    delete_data('index_data', ref_date, conn2)
    insert_data('index_data', df, conn2)
    return 0


def update_risk_factor_300(ds, **kwargs):
    ref_date = kwargs['next_execution_date']\

    if not isBizDay('china.sse', ref_date):
        logger.info("{0} is not a business day".format(ref_date))
        return 0

    ref_date = ref_date.strftime('%Y-%m-%d')

    conn1 = create_ms_engine('PortfolioManagements300')
    df = fetch_date('RiskFactor', ref_date, conn1)

    conn2 = create_my_engine()

    delete_data('risk_factor_300', ref_date, conn2)
    insert_data('risk_factor_300', df, conn2)
    return 0


def update_risk_factor_500(ds, **kwargs):
    ref_date = kwargs['next_execution_date']\

    if not isBizDay('china.sse', ref_date):
        logger.info("{0} is not a business day".format(ref_date))
        return 0

    ref_date = ref_date.strftime('%Y-%m-%d')

    conn1 = create_ms_engine('PortfolioManagements500')
    df = fetch_date('RiskFactor', ref_date, conn1)

    conn2 = create_my_engine()

    delete_data('risk_factor_500', ref_date, conn2)
    insert_data('risk_factor_500', df, conn2)
    return 0


def update_factor_indicator(ds, **kwargs):
    ref_date = kwargs['next_execution_date']\

    if not isBizDay('china.sse', ref_date):
        logger.info("{0} is not a business day".format(ref_date))
        return 0

    ref_date = ref_date.strftime('%Y-%m-%d')

    conn1 = create_ms_engine('FactorPerformance')
    df = fetch_date('FactorIndicator_500', ref_date, conn1)

    conn2 = create_my_engine()

    delete_data('factor_indicator', ref_date, conn2)
    insert_data('factor_indicator', df, conn2)
    return 0


run_this1 = PythonOperator(
    task_id='update_factor_data',
    provide_context=True,
    python_callable=update_factor_data,
    dag=dag
)

run_this2 = PythonOperator(
    task_id='update_index_components',
    provide_context=True,
    python_callable=update_index_components,
    dag=dag
)

run_this3 = PythonOperator(
    task_id='update_index_data',
    provide_context=True,
    python_callable=update_index_data,
    dag=dag
)

run_this4 = PythonOperator(
    task_id='update_risk_factor_300',
    provide_context=True,
    python_callable=update_risk_factor_300,
    dag=dag
)

run_this5 = PythonOperator(
    task_id='update_risk_factor_500',
    provide_context=True,
    python_callable=update_risk_factor_500,
    dag=dag
)

run_this6 = PythonOperator(
    task_id='update_factor_indicator',
    provide_context=True,
    python_callable=update_factor_indicator,
    dag=dag
)
