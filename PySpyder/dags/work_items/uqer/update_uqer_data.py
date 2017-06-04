# -*- coding: utf-8 -*-
"""
Created on 2017-5-20

@author: cheng.li
"""

import datetime as dt
import uqer
import sqlalchemy
import pandas as pd
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from uqer import DataAPI as api
from simpleutils import CustomLogger


logger = CustomLogger('MULTI_FACTOR', 'info')

start_date = dt.datetime(2016, 12, 31)
dag_name = 'update_uqer_data'

default_args = {
    'owner': 'wegamekinglc',
    'depends_on_past': False,
    'start_date': start_date
}

dag = DAG(
    dag_id=dag_name,
    default_args=default_args,
    schedule_interval='0 18 * * 1,2,3,4,5'
)


_ = uqer.Client(username='13817268186', password='we083826')
engine1 = sqlalchemy.create_engine('mysql+mysqldb://sa:We051253524522@rm-bp1psdz5615icqc0y.mysql.rds.aliyuncs.com/multifactor?charset=utf8')
engine2 = sqlalchemy.create_engine('mysql+mysqldb://sa:We051253524522@rm-bp1psdz5615icqc0y.mysql.rds.aliyuncs.com/uqer?charset=utf8')


def process_date(ds):
    logger.info("Loading data at {0}".format(ds))
    this_date = dt.datetime.strptime(ds, '%Y-%m-%d')
    ref_date = this_date.strftime('%Y%m%d')
    return ref_date, this_date


def update_uqer_factors(ds, **kwargs):
    ref_date, _ = process_date(ds)

    table = 'factor_uqer'
    df = api.MktStockFactorsOneDayProGet(tradeDate=ref_date)
    df.rename(columns={'tradeDate': 'Date', 'ticker': 'Code'}, inplace=True)
    df.Code = df.Code.astype(int)
    del df['secID']

    engine1.execute("delete from {0} where Date = '{1}';".format(table, ref_date))
    df.to_sql(table, engine1, index=False, if_exists='append')

    table = 'factors'
    engine2.execute("delete from {0} where Date = '{1}';".format(table, ref_date))
    df.to_sql(table, engine2, index=False, if_exists='append')


def update_uqer_market(ds, **kwargs):
    ref_date, _ = process_date(ds)

    table = 'market'
    df = api.MktEqudGet(tradeDate=ref_date)
    df.rename(columns={'tradeDate': 'Date', 'ticker': 'Code'}, inplace=True)
    df.Code = df.Code.astype(int)
    del df['secID']
    engine2.execute("delete from {0} where Date = '{1}';".format(table, ref_date))
    df.to_sql(table, engine2, index=False, if_exists='append')


def update_uqer_index_components(ds, **kwargs):
    ref_date, this_date = process_date(ds)

    table = 'index_components'
    index_codes = ['000001', '000300', '000905', '000016', '399005', '399006']

    total_data = pd.DataFrame()
    for index in index_codes:
        df = api.IdxCloseWeightGet(ticker=index,
                                   beginDate=dt.datetime(this_date.year - 1, this_date.month, this_date.day).strftime(
                                       '%Y%m%d'), endDate=ref_date)
        df = df[df.effDate == df.effDate.unique()[-1]]
        df.rename(columns={'ticker': 'indexCode',
                           'secShortName': 'indexShortName',
                           'consTickerSymbol': 'Code',
                           'consExchangeCD': 'exchangeCD',
                           'consShortName': 'secShortName'}, inplace=True)
        df['indexCode'] = df.indexCode.astype(int)
        df['Code'] = df.Code.astype(int)
        df['Date'] = this_date
        del df['secID']
        del df['consID']
        total_data = total_data.append(df)

    engine2.execute("delete from {0} where Date = '{1}';".format(table, ref_date))
    total_data.to_sql(table, engine2, index=False, if_exists='append')


def update_uqer_risk_model(ds, **kwargs):
    ref_date, this_date = process_date(ds)

    table = 'risk_exposure'
    df = api.RMExposureDayGet(tradeDate=ref_date)
    df.rename(columns={'tradeDate': 'Date', 'ticker': 'Code'}, inplace=True)
    df.Code = df.Code.astype(int)
    del df['secID']
    engine2.execute("delete from {0} where Date = '{1}';".format(table, ref_date))
    df.to_sql(table, engine2, index=False, if_exists='append')

    table = 'risk_return'
    df = api.RMFactorRetDayGet(tradeDate=ref_date)
    df.rename(columns={'tradeDate': 'Date'}, inplace=True)
    engine2.execute("delete from {0} where Date = '{1}';".format(table, ref_date))
    df.to_sql(table, engine2, index=False, if_exists='append')

    table = 'specific_return'
    df = api.RMSpecificRetDayGet(tradeDate=ref_date)
    df.rename(columns={'tradeDate': 'Date', 'ticker': 'Code'}, inplace=True)
    df.Code = df.Code.astype(int)
    del df['secID']
    engine2.execute("delete from {0} where Date = '{1}';".format(table, ref_date))
    df.to_sql(table, engine2, index=False, if_exists='append')

    table = 'risk_cov_day'
    df = api.RMCovarianceDayGet(tradeDate=ref_date)
    df.rename(columns={'tradeDate': 'Date'}, inplace=True)
    engine2.execute("delete from {0} where Date = '{1}';".format(table, ref_date))
    df.to_sql(table, engine2, index=False, if_exists='append')

    table = 'risk_cov_short'
    df = api.RMCovarianceShortGet(tradeDate=ref_date)
    df.rename(columns={'tradeDate': 'Date'}, inplace=True)
    engine2.execute("delete from {0} where Date = '{1}';".format(table, ref_date))
    df.to_sql(table, engine2, index=False, if_exists='append')

    table = 'risk_cov_long'
    df = api.RMCovarianceLongGet(tradeDate=ref_date)
    df.rename(columns={'tradeDate': 'Date'}, inplace=True)
    engine2.execute("delete from {0} where Date = '{1}';".format(table, ref_date))
    df.to_sql(table, engine2, index=False, if_exists='append')

    table = 'specific_risk_day'
    df = api.RMSriskDayGet(tradeDate=ref_date)
    df.rename(columns={'tradeDate': 'Date', 'ticker': 'Code'}, inplace=True)
    df.Code = df.Code.astype(int)
    del df['secID']
    engine2.execute("delete from {0} where Date = '{1}';".format(table, ref_date))
    df.to_sql(table, engine2, index=False, if_exists='append')

    table = 'specific_risk_short'
    df = api.RMSriskShortGet(tradeDate=ref_date)
    df.rename(columns={'tradeDate': 'Date', 'ticker': 'Code'}, inplace=True)
    df.Code = df.Code.astype(int)
    del df['secID']
    engine2.execute("delete from {0} where Date = '{1}';".format(table, ref_date))
    df.to_sql(table, engine2, index=False, if_exists='append')

    table = 'specific_risk_long'
    df = api.RMSriskLongGet(tradeDate=ref_date)
    df.rename(columns={'tradeDate': 'Date', 'ticker': 'Code'}, inplace=True)
    df.Code = df.Code.astype(int)
    del df['secID']
    engine2.execute("delete from {0} where Date = '{1}';".format(table, ref_date))
    df.to_sql(table, engine2, index=False, if_exists='append')


_ = PythonOperator(
    task_id='update_uqer_factors',
    provide_context=True,
    python_callable=update_uqer_factors,
    dag=dag
)


_ = PythonOperator(
    task_id='update_uqer_market',
    provide_context=True,
    python_callable=update_uqer_market,
    dag=dag
)


_ = PythonOperator(
    task_id='update_uqer_index_components',
    provide_context=True,
    python_callable=update_uqer_index_components,
    dag=dag
)


_ = PythonOperator(
    task_id='update_uqer_risk_model',
    provide_context=True,
    python_callable=update_uqer_risk_model,
    dag=dag
)


if __name__ == '__main__':
    update_uqer_risk_model(ds='2017-05-31')