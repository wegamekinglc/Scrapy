# -*- coding: utf-8 -*-
"""
Created on 2017-5-17

@author: cheng.li
"""

import datetime as dt
import sqlalchemy as sa
import numpy as np
import pandas as pd
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from PyFin.api import isBizDay
from PyFin.api import advanceDateByCalendar
from simpleutils import CustomLogger
from alphamind.examples.config import risk_factors_500
from alphamind.data.standardize import standardize
from alphamind.data.winsorize import winsorize_normal
from alphamind.data.neutralize import neutralize
from alphamind.portfolio.linearbuilder import linear_build

logger = CustomLogger('MULTI_FACTOR', 'info')


start_date = dt.datetime(2012, 1, 1)
dag_name = 'update_factor_analysis'

default_args = {
    'owner': 'wegamekinglc',
    'depends_on_past': False,
    'start_date': start_date
}

dag = DAG(
    dag_id=dag_name,
    default_args=default_args,
    schedule_interval='0 19 * * 1,2,3,4,5'
)

source_db = sa.create_engine('mysql+mysqldb://user:pwd@host/multifactor?charset=utf8')
destination_db = sa.create_engine('mysql+mysqldb://user:pwd@host/factor_analysis?charset=utf8')


def get_industry_codes(ref_date, engine):
    return pd.read_sql("select Code, 申万一级行业 from factor_data where Date = '{0}'".format(ref_date), engine)


def get_risk_factors(ref_date, engine):
    risk_factor_list = ','.join(risk_factors_500)
    risk_factors = pd.read_sql("select Code, {0} from risk_factor_500 where Date = '{1}'".format(risk_factor_list, ref_date), engine)
    risk_factors['Market'] = 1.
    risk_cols = risk_factors_500 + ['Market']
    return risk_cols, risk_factors


def get_security_returns(ref_date, engine):
    return pd.read_sql("select Code, D1LogReturn, isTradable from return_500 where Date = '{0}'".format(ref_date), engine)


def get_index_components(ref_date, engine):
    df = pd.read_sql("select Code, 500Weight from index_components where Date = '{0}' and 500Weight > 0".format(ref_date), engine)
    df.rename(columns={'500Weight': 'zz500'}, inplace=True)
    df['zz500'] /= 100.
    return df[['Code', 'zz500']]


def get_all_the_factors(ref_date, engine, codes):
    codes_list = ','.join([str(c) for c in codes])
    common_factors = pd.read_sql("select * from factor_data where Date = '{0}' and Code in ({1})".format(ref_date, codes_list), engine)
    del common_factors['Date']
    del common_factors['申万一级行业']
    del common_factors['申万二级行业']
    del common_factors['申万三级行业']
    prod_factors = pd.read_sql("select * from prod_500 where Date = '{0}' and Code in ({1})".format(ref_date, codes_list), engine)
    del prod_factors['Date']
    common_500 = pd.read_sql("select * from common_500 where Date = '{0}' and Code in ({1})".format(ref_date, codes_list), engine)
    del common_500['Date']

    total_factors = pd.merge(common_factors, prod_factors, on=['Code'], how='left')
    total_factors = pd.merge(total_factors, common_500, on=['Code'], how='left')

    return total_factors.dropna(axis=1)


def merge_data(total_factors, industry_codes, risk_factors, index_components, daily_returns):
    factor_cols = total_factors.columns[2:].tolist()
    total_data = pd.merge(total_factors, industry_codes, on=['Code'])
    total_data = pd.merge(total_data, risk_factors, on=['Code'])
    total_data = pd.merge(total_data, index_components, on=['Code'])
    total_data = pd.merge(total_data, daily_returns, on=['Code'])
    total_data.dropna(inplace=True)

    if len(total_data) < 500:
        raise ValueError('Data is missing for some codes')

    return factor_cols, total_data


def process_data(total_data, factor_cols, risk_cols):
    risk_values = total_data[risk_cols].values
    factor_values = total_data[factor_cols].values
    processed_values = neutralize(risk_values,
                                  standardize(winsorize_normal(factor_values)))
    return processed_values


def build_portfolio(er_values, total_data, factor_cols, risk_cols):
    bm = total_data['zz500'].values
    lbound = np.zeros(len(bm))
    ubound = 0.01 + bm
    lbound_exposure = -0.01
    ubound_exposure = 0.01
    risk_exposure = total_data[risk_cols].values

    is_trading = total_data['isTradable'].values
    ubound[~is_trading] = 0.

    factor_pos = {}

    for i, name in enumerate(factor_cols):
        er = er_values[:, i]
        status, value, ret = linear_build(er,
                                          lbound=lbound,
                                          ubound=ubound,
                                          risk_exposure=risk_exposure,
                                          bm=bm,
                                          risk_target=(lbound_exposure, ubound_exposure),
                                          solver='GLPK')
        if status != 'optimal':
            raise ValueError('target is not feasible')
        else:
            factor_pos[name] = ret

    return pd.DataFrame(factor_pos, index=total_data.Code)


def settlement(ref_date, pos_df, bm, returns, type='risk_neutral'):
    ret_series = [(pos_df[name].values - bm) @ returns for name in pos_df.columns]
    return pd.DataFrame({'Date': ref_date,
                         'er': ret_series,
                         'portfolio': pos_df.columns,
                         'type': type})


def upload(ref_date, return_table, engine):
    engine.execute("delete from performance where Date = '{0}'".format(ref_date))
    return_table.to_sql('performance', engine, if_exists='append', index=False)


def update_factor_performance(ds, **kwargs):
    ref_date = kwargs['next_execution_date']
    if not isBizDay('china.sse', ref_date):
        logger.info("{0} is not a business day".format(ref_date))
        return 0

    ref_date = advanceDateByCalendar('china.sse', ref_date, '-2b')
    ref_date = ref_date.strftime('%Y-%m-%d')

    industry_codes = get_industry_codes(ref_date, source_db)
    risk_cols, risk_factors = get_risk_factors(ref_date, source_db)
    index_components = get_index_components(ref_date, source_db)
    daily_returns = get_security_returns(ref_date, source_db)
    total_factors = get_all_the_factors(ref_date, source_db, index_components.Code)
    factor_cols, total_data = merge_data(total_factors, industry_codes, risk_factors, index_components, daily_returns)
    processed_values = process_data(total_data, factor_cols, risk_cols)

    pos_df = build_portfolio(processed_values, total_data, factor_cols, risk_cols)
    return_table = settlement(ref_date, pos_df, total_data['zz500'].values, total_data['D1LogReturn'].values)
    upload(ref_date, return_table, destination_db)


run_this1 = PythonOperator(
    task_id='update_factor_performance',
    provide_context=True,
    python_callable=update_factor_performance,
    dag=dag
)