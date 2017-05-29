# -*- coding: utf-8 -*-
"""
Created on 2017-5-29

@author: cheng.li
"""

import datetime as dt
import sqlalchemy as sa
import numpy as np
import pandas as pd
from PyFin.api import isBizDay
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from PyFin.api import advanceDateByCalendar
from simpleutils import CustomLogger
from alphamind.examples.config import risk_factors_300
from alphamind.analysis.factoranalysis import factor_analysis

logger = CustomLogger('MULTI_FACTOR', 'info')

engine = sa.create_engine(
    'mysql+pymysql://sa:We051253524522@rm-bp1psdz5615icqc0yo.mysql.rds.aliyuncs.com:3306/multifactor?charset=utf8')

destination = sa.create_engine('mysql+pymysql://sa:We051253524522@rm-bp1psdz5615icqc0yo.mysql.rds.aliyuncs.com/factor_analysis?charset=utf8')


def prod_300_one_day(factor_name, ref_date, use_only_index_components=False, risk_neutral=True):
    risk_factors = ','.join(['risk_factor_500.' + name for name in risk_factors_300])

    sql = "select prod_300.Code, prod_300.{factor_name}, factor_data.申万一级行业, {risk_factors}," \
          " return_500.D1LogReturn, return_500.isTradable from prod_300".format(factor_name=factor_name,
                                                                                  risk_factors=risk_factors)
    sql += " join factor_data on prod_300.Date = factor_data.Date and prod_300.Code = factor_data.Code" \
           " join risk_factor_500 on prod_300.Date = risk_factor_500.Date and prod_300.Code = risk_factor_500.Code" \
           " join return_500 on prod_300.Date = return_500.Date and prod_300.Code = return_500.Code" \
           " where prod_300.Date = '{ref_date}'".format(ref_date=ref_date)

    df1 = pd.read_sql(sql, engine).dropna()

    if df1.empty:
        return None, None

    df2 = pd.read_sql("select Code, 500Weight / 100. as benchmark from index_components "
                      "where Date ='{ref_date}'".format(ref_date=ref_date),
                      engine)

    df = pd.merge(df1, df2, on=['Code'], how='left').fillna(0.)

    if use_only_index_components:
        df = df[df.benchmark != 0.]

    factors = df[['Code', factor_name]].set_index('Code')[factor_name]
    industry = df['申万一级行业'].values
    d1returns = df['D1LogReturn'].values
    is_tradable = df['isTradable'].values
    benchmark = df['benchmark'].values
    risk_exp = df[risk_factors_300].values
    risk_exp = np.concatenate([risk_exp, np.ones((len(risk_exp), 1))], axis=1)

    if risk_neutral:
        weights, analysis = factor_analysis(factors=factors,
                                            industry=industry,
                                            d1returns=d1returns,
                                            detail_analysis=True,
                                            benchmark=benchmark,
                                            risk_exp=risk_exp,
                                            is_tradable=is_tradable)
    else:
        weights, analysis = factor_analysis(factors=factors,
                                            industry=industry,
                                            d1returns=d1returns,
                                            detail_analysis=True,
                                            risk_exp=risk_exp,
                                            is_tradable=is_tradable)
    return weights, analysis


def prod_300_analysis(factor_name, ref_date, use_only_index_components=False, risk_neutral=True):

    previous_day = advanceDateByCalendar('china.sse', ref_date, '-1b').strftime('%Y-%m-%d')

    weights, analysis = prod_300_one_day(factor_name, ref_date, use_only_index_components, risk_neutral)

    if weights is None:
        logger.warning("No data for '{0}' on {1}".format(factor_name, ref_date))
        return

    previous_weight, _ = prod_300_one_day(factor_name, previous_day, use_only_index_components, risk_neutral)

    pos_diff_dict = {}

    if weights is not None:
        for ind in weights.industry.unique():
            pos_series = weights.loc[weights.industry == ind, 'weight']
            if previous_weight is not None:
                last_series = previous_weight.loc[previous_weight.industry == ind, 'weight']
                pos_diff = pos_series.sub(last_series, fill_value=0)
            else:
                pos_diff = pos_series

            pos_diff_dict[ind] = pos_diff.abs().sum()

        pos_diff_dict['total'] = sum(pos_diff_dict.values())

    inds = list(pos_diff_dict.keys())
    pos_diff_series = pd.DataFrame({'turn_over': [pos_diff_dict[ind] for ind in inds]}, index=inds)

    return_table = pd.merge(analysis, pos_diff_series, left_index=True, right_index=True)
    return_table.index.name = 'industry'
    return_table['Date'] = dt.datetime.strptime(ref_date, '%Y-%m-%d')
    return_table['portfolio'] = factor_name
    if risk_neutral:
        return_table['type'] = 'risk_neutral'
    else:
        return_table['type'] = 'top_100'
    return_table.reset_index(inplace=True)
    return return_table


def upload(ref_date, return_table, engine, table, factor_name, build_type):
    engine.execute("delete from {1} where Date = '{0}' and type = '{2}' and portfolio = '{3}'".format(ref_date, table, build_type, factor_name))
    return_table.to_sql(table, engine, if_exists='append', index=False)


def create_factor_analysis(ds, **kwargs):
    ref_date = kwargs['next_execution_date']
    if not isBizDay('china.sse', ref_date):
        logger.info("{0} is not a business day".format(ref_date))
        return

    ref_date = advanceDateByCalendar('china.sse', ref_date, '-2b').strftime('%Y-%m-%d')

    factor_name = kwargs['factor_name']
    logger.info("updating '{0}' on {1}".format(factor_name, ref_date))

    # small universe, risk_neutral
    return_table = prod_300_analysis(factor_name, ref_date, use_only_index_components=True, risk_neutral=True)
    if return_table is not None:
        upload(ref_date, return_table, destination, 'performance_300', factor_name, 'risk_neutral')

    # small universe, top_100
    return_table = prod_300_analysis(factor_name, ref_date, use_only_index_components=True, risk_neutral=False)
    if return_table is not None:
        upload(ref_date, return_table, destination, 'performance_300', factor_name, 'top_100')

    # small universe, risk_neutral
    return_table = prod_300_analysis(factor_name, ref_date, use_only_index_components=False, risk_neutral=True)
    if return_table is not None:
        upload(ref_date, return_table, destination, 'performance_big_universe_300', factor_name, 'risk_neutral')

    # small universe, top_100
    return_table = prod_300_analysis(factor_name, ref_date, use_only_index_components=False, risk_neutral=False)
    if return_table is not None:
        upload(ref_date, return_table, destination, 'performance_big_universe_300', factor_name, 'top_100')


factor_table = pd.read_sql('Describe prod_300', engine)

start_date = dt.datetime(2012, 1, 1)
dag_name = 'update_prod_300_analysis'

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


for factor_name in factor_table.Field:
    if factor_name != 'Date' and factor_name != 'Code':
        task = PythonOperator(task_id=factor_name,
                              provide_context=True,
                              python_callable=create_factor_analysis,
                              op_kwargs={'factor_name': factor_name},
                              dag=dag)


if __name__ == '__main__':
    create_factor_analysis(None, next_execution_date='2008-01-04', factor_name='CFinc1')