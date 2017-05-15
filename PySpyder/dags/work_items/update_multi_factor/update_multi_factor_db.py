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
from PyFin.api import advanceDateByCalendar
from PyFin.api import bizDatesList


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

date_formatted_tables = {'FactorIndicator_500', 'Portfolio_LongTop_500'}
date_lowered = {'AlphaFactors_Difeiyue'}


def create_ms_engine(db):
    ms_user = ''
    ms_pwd = ''
    return sqlalchemy.create_engine(
        'mssql+pymssql://{0}:{1}@10.63.6.219/{2}?charset=cp936'.format(ms_user, ms_pwd, db))


def create_my_engine():
    my_user = ''
    my_pwd = ''
    return sqlalchemy.create_engine(
        'mysql+pymysql://{0}:{1}@rm-bp1psdz5615icqc0yo.mysql.rds.aliyuncs.com/multifactor?charset=utf8'.format(my_user, my_pwd))


def create_my_engine2():
    my_user = 'sa'
    my_pwd = 'we083826'
    return sqlalchemy.create_engine(
        'mysql+pymysql://{0}:{1}@10.63.6.176/multifactor?charset=utf8'.format(my_user, my_pwd))


def fetch_date(table, query_date, engine):
    query_date = query_date.replace('-', '')
    if table in date_formatted_tables:
        sql = "select * from {0} where Date = '{1}'".format(table, query_date)
        df = pd.read_sql_query(sql, engine)
    elif table in date_lowered:
        sql = "select * from {0} where date = {1}".format(table, query_date)
        df = pd.read_sql_query(sql, engine)
        df.rename(columns={'date': 'Date', 'code': 'Code'}, inplace=True)
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
    ref_date = kwargs['next_execution_date']

    if not isBizDay('china.sse', ref_date):
        logger.info("{0} is not a business day".format(ref_date))
        return 0

    ref_date = ref_date.strftime('%Y-%m-%d')

    conn1 = create_ms_engine('MultiFactor')
    df = fetch_date('FactorData', ref_date, conn1)

    conn2 = create_my_engine()

    delete_data('factor_data', ref_date, conn2)
    insert_data('factor_data', df, conn2)

    conn3 = create_my_engine2()
    delete_data('factor_data', ref_date, conn3)
    insert_data('factor_data', df, conn3)

    return 0


def update_index_components(ds, **kwargs):
    ref_date = kwargs['next_execution_date']

    if not isBizDay('china.sse', ref_date):
        logger.info("{0} is not a business day".format(ref_date))
        return 0

    ref_date = ref_date.strftime('%Y-%m-%d')

    conn1 = create_ms_engine('MultiFactor')
    df = fetch_date('IndexComponents', ref_date, conn1)

    conn2 = create_my_engine()

    delete_data('index_components', ref_date, conn2)
    insert_data('index_components', df, conn2)

    conn3 = create_my_engine2()
    delete_data('index_components', ref_date, conn3)
    insert_data('index_components', df, conn3)
    return 0


def update_index_data(ds, **kwargs):
    ref_date = kwargs['next_execution_date']

    if not isBizDay('china.sse', ref_date):
        logger.info("{0} is not a business day".format(ref_date))
        return 0

    ref_date = ref_date.strftime('%Y-%m-%d')

    conn1 = create_ms_engine('MultiFactor')
    df = fetch_date('StockIndices', ref_date, conn1)

    conn2 = create_my_engine()

    delete_data('index_data', ref_date, conn2)
    insert_data('index_data', df, conn2)

    conn3 = create_my_engine2()
    delete_data('index_data', ref_date, conn3)
    insert_data('index_data', df, conn3)
    return 0


def update_risk_factor_300(ds, **kwargs):
    ref_date = kwargs['next_execution_date']

    if not isBizDay('china.sse', ref_date):
        logger.info("{0} is not a business day".format(ref_date))
        return 0

    ref_date = ref_date.strftime('%Y-%m-%d')

    conn1 = create_ms_engine('PortfolioManagements300')
    df = fetch_date('RiskFactor', ref_date, conn1)

    conn2 = create_my_engine()

    delete_data('risk_factor_300', ref_date, conn2)
    insert_data('risk_factor_300', df, conn2)

    conn3 = create_my_engine2()
    delete_data('risk_factor_300', ref_date, conn3)
    insert_data('risk_factor_300', df, conn3)
    return 0


def update_risk_factor_500(ds, **kwargs):
    ref_date = kwargs['next_execution_date']

    if not isBizDay('china.sse', ref_date):
        logger.info("{0} is not a business day".format(ref_date))
        return 0

    ref_date = ref_date.strftime('%Y-%m-%d')

    conn1 = create_ms_engine('PortfolioManagements500')
    df = fetch_date('RiskFactor', ref_date, conn1)

    conn2 = create_my_engine()

    delete_data('risk_factor_500', ref_date, conn2)
    insert_data('risk_factor_500', df, conn2)

    conn3 = create_my_engine2()
    delete_data('risk_factor_500', ref_date, conn3)
    insert_data('risk_factor_500', df, conn3)
    return 0


def update_factor_indicator(ds, **kwargs):
    ref_date = kwargs['next_execution_date']

    if not isBizDay('china.sse', ref_date):
        logger.info("{0} is not a business day".format(ref_date))
        return 0
    
    ref_date = advanceDateByCalendar('china.sse', ref_date, '-2b')
    ref_date = ref_date.strftime('%Y-%m-%d')

    conn1 = create_ms_engine('FactorPerformance')
    df = fetch_date('FactorIndicator_500', ref_date, conn1)

    conn2 = create_my_engine()

    delete_data('factor_indicator', ref_date, conn2)
    insert_data('factor_indicator', df, conn2)

    conn3 = create_my_engine2()
    delete_data('factor_indicator', ref_date, conn3)
    insert_data('factor_indicator', df, conn3)
    return 0


def update_prod_300(ds, **kwargs):
    ref_date = kwargs['next_execution_date']

    if not isBizDay('china.sse', ref_date):
        logger.info("{0} is not a business day".format(ref_date))
        return 0

    ref_date = ref_date.strftime('%Y-%m-%d')

    conn1 = create_ms_engine('PortfolioManagements300')
    df = fetch_date('AlphaFactors_Difeiyue', ref_date, conn1)

    conn2 = create_my_engine()

    delete_data('prod_300', ref_date, conn2)
    insert_data('prod_300', df, conn2)

    conn3 = create_my_engine2()
    delete_data('prod_300', ref_date, conn3)
    insert_data('prod_300', df, conn3)
    return 0


def update_prod_500(ds, **kwargs):
    ref_date = kwargs['next_execution_date']

    if not isBizDay('china.sse', ref_date):
        logger.info("{0} is not a business day".format(ref_date))
        return 0

    ref_date = ref_date.strftime('%Y-%m-%d')

    conn1 = create_ms_engine('PortfolioManagements500')
    df = fetch_date('AlphaFactors_Difeiyue', ref_date, conn1)

    conn2 = create_my_engine()

    delete_data('prod_500', ref_date, conn2)
    insert_data('prod_500', df, conn2)

    conn3 = create_my_engine2()
    delete_data('prod_500', ref_date, conn3)
    insert_data('prod_500', df, conn3)
    return 0


def update_trade_data(ds, **kwargs):
    ref_date = kwargs['next_execution_date']

    if not isBizDay('china.sse', ref_date):
        logger.info("{0} is not a business day".format(ref_date))
        return 0

    ref_date = ref_date.strftime('%Y-%m-%d')

    conn1 = create_ms_engine('MultiFactor')
    df = fetch_date('TradingInfo1', ref_date, conn1)

    conn2 = create_my_engine()

    delete_data('trade_data', ref_date, conn2)
    insert_data('trade_data', df, conn2)

    conn3 = create_my_engine2()
    delete_data('trade_data', ref_date, conn3)
    insert_data('trade_data', df, conn3)
    return 0


def update_portfolio_long_top(ds, **kwargs):
    ref_date = kwargs['next_execution_date']

    if not isBizDay('china.sse', ref_date):
        logger.info("{0} is not a business day".format(ref_date))
        return 0

    ref_date = ref_date.strftime('%Y-%m-%d')

    conn1 = create_ms_engine('FactorPerformance')
    df = fetch_date('Portfolio_LongTop_500', ref_date, conn1)

    conn2 = create_my_engine()

    delete_data('portfolio_longtop', ref_date, conn2)
    insert_data('portfolio_longtop', df, conn2)

    conn3 = create_my_engine2()
    delete_data('portfolio_longtop', ref_date, conn3)
    insert_data('portfolio_longtop', df, conn3)
    return 0


def update_return_data_500(ds, **kwargs):
    ref_date = kwargs['next_execution_date']

    if not isBizDay('china.sse', ref_date):
        logger.info("{0} is not a business day".format(ref_date))
        return 0

    start_date = advanceDateByCalendar('china.sse', ref_date, '-30b')

    for date in bizDatesList('china.sse', start_date, ref_date):

        date = date.strftime('%Y-%m-%d')

        conn1 = create_ms_engine('PortfolioManagements500')
        df = fetch_date('StockReturns', date, conn1)

        conn2 = create_my_engine()

        delete_data('return_500', date, conn2)
        insert_data('return_500', df, conn2)

        conn3 = create_my_engine2()
        delete_data('return_500', date, conn3)
        insert_data('return_500', df, conn3)
    return 0


def update_return_data_300(ds, **kwargs):
    ref_date = kwargs['next_execution_date']

    if not isBizDay('china.sse', ref_date):
        logger.info("{0} is not a business day".format(ref_date))
        return 0

    start_date = advanceDateByCalendar('china.sse', ref_date, '-30b')

    for date in bizDatesList('china.sse', start_date, ref_date):
     
        date = date.strftime('%Y-%m-%d')

        conn1 = create_ms_engine('PortfolioManagements300')
        df = fetch_date('StockReturns', date, conn1)

        conn2 = create_my_engine()

        delete_data('return_300', date, conn2)
        insert_data('return_300', df, conn2)

        conn3 = create_my_engine2()
        delete_data('return_300', date, conn3)
        insert_data('return_300', df, conn3)
    return 0


def update_common_500(ds, **kwargs):
    ref_date = kwargs['next_execution_date']

    if not isBizDay('china.sse', ref_date):
        logger.info("{0} is not a business day".format(ref_date))
        return 0

    ref_date = ref_date.strftime('%Y-%m-%d')

    conn1 = create_ms_engine('PortfolioManagements500')
    df = fetch_date('AlphaFactors_Common', ref_date, conn1)

    conn2 = create_my_engine()

    delete_data('common_500', ref_date, conn2)
    insert_data('common_500', df, conn2)

    conn3 = create_my_engine2()
    delete_data('common_500', ref_date, conn3)
    insert_data('common_500', df, conn3)
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

run_this7 = PythonOperator(
    task_id='update_prod_300',
    provide_context=True,
    python_callable=update_prod_300,
    dag=dag
)

run_this8 = PythonOperator(
    task_id='update_prod_500',
    provide_context=True,
    python_callable=update_prod_500,
    dag=dag
)

run_this9 = PythonOperator(
    task_id='update_trade_data',
    provide_context=True,
    python_callable=update_trade_data,
    dag=dag
)

run_this10 = PythonOperator(
    task_id='update_portfolio_long_top',
    provide_context=True,
    python_callable=update_portfolio_long_top,
    dag=dag
)

run_this11 = PythonOperator(
    task_id='update_return_data_300',
    provide_context=True,
    python_callable=update_return_data_300,
    dag=dag
)

run_this12 = PythonOperator(
    task_id='update_return_data_500',
    provide_context=True,
    python_callable=update_return_data_500,
    dag=dag
)

run_this13 = PythonOperator(
    task_id='update_common_500',
    provide_context=True,
    python_callable=update_common_500,
    dag=dag
)

