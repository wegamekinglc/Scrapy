# -*- coding: utf-8 -*-
"""
Created on 2017-5-19

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
from alphamind.examples.config import risk_factors_300
from alphamind.data.standardize import standardize
from alphamind.data.winsorize import winsorize_normal
from alphamind.data.neutralize import neutralize
from alphamind.portfolio.linearbuilder import linear_build

alpha_strategy = {}


logger = CustomLogger('MULTI_FACTOR', 'info')


start_date = dt.datetime(2017, 1, 5)
dag_name = 'update_factor_analysis_uqer_300'

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


source_db = sa.create_engine('mysql+mysqldb://sa:We051253524522@rm-bp1psdz5615icqc0yo.mysql.rds.aliyuncs.com/multifactor?charset=utf8')
destination_db = sa.create_engine('mysql+mysqldb://sa:We051253524522@rm-bp1psdz5615icqc0yo.mysql.rds.aliyuncs.com/factor_analysis?charset=utf8')


def get_industry_codes(ref_date, engine):
    return pd.read_sql("select Code, 申万一级行业 from factor_data where Date = '{0}'".format(ref_date), engine)


def get_risk_factors(ref_date, engine):
    risk_factor_list = ','.join(risk_factors_300)
    risk_factors = pd.read_sql("select Code, {0} from risk_factor_300 where Date = '{1}'".format(risk_factor_list, ref_date), engine)
    risk_factors['Market'] = 1.
    risk_cols = risk_factors_300 + ['Market']
    return risk_cols, risk_factors


def get_security_returns(ref_date, engine):
    return pd.read_sql("select Code, D1LogReturn, isTradable from return_300 where Date = '{0}'".format(ref_date), engine)


def get_index_components(ref_date, engine):
    df = pd.read_sql("select Code, 300Weight from index_components where Date = '{0}' and 300Weight > 0".format(ref_date), engine)
    df.rename(columns={'300Weight': 'hs300'}, inplace=True)
    df['hs300'] /= 100.
    return df[['Code', 'hs300']]


def get_all_the_factors(ref_date, engine, codes=None):
    if codes:
        codes_list = ','.join([str(c) for c in codes])
    else:
        codes_list = None
    if codes_list:
        total_factors = pd.read_sql("select * from factor_uqer where Date = '{0}' and Code in ({1})".format(ref_date, codes_list), engine)
        del total_factors['Date']
    else:
        total_factors = pd.read_sql("select * from factor_uqer where Date = '{0}'".format(ref_date), engine)

    total_factors.dropna(axis=1, how='all', inplace=True)
    total_factors.fillna(total_factors.mean(), inplace=True)
    return total_factors


def merge_data(total_factors, industry_codes, risk_factors, index_components, daily_returns):
    factor_cols = total_factors.columns.difference(['Date', 'Code', '申万一级行业'])
    total_data = pd.merge(total_factors, index_components, on=['Code'], how='left')
    total_data.fillna(0, inplace=True)
    total_data = pd.merge(total_data, industry_codes, on=['Code'])
    total_data = pd.merge(total_data, risk_factors, on=['Code'])
    total_data = pd.merge(total_data, daily_returns, on=['Code'])
    total_data.dropna(inplace=True)

    return factor_cols, total_data


def process_data(total_data, factor_cols, risk_cols):
    risk_values = total_data[risk_cols].values
    factor_values = total_data[factor_cols].values
    processed_values = np.zeros(factor_values.shape)

    for i in range(processed_values.shape[1]):
        try:
            processed_values[:, i] = neutralize(risk_values,
                                                standardize(winsorize_normal(factor_values[:, [i]]))).flatten()
        except np.linalg.linalg.LinAlgError:
            processed_values[:, i] = neutralize(risk_values,
                                                winsorize_normal(factor_values[:, [i]])).flatten()
    return processed_values


def build_portfolio(er_values, total_data, factor_cols, risk_cols, risk_lbound, risk_ubound):
    bm = total_data['hs300'].values
    lbound = np.zeros(len(bm))
    ubound = 0.0075 + bm
    lbound_exposure = risk_lbound
    ubound_exposure = risk_ubound
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

    for name in alpha_strategy:
        er = np.zeros(len(total_data))
        for f in alpha_strategy[name]:
            er += alpha_strategy[name][f] * total_data[f].values

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

    res = pd.DataFrame(factor_pos, index=total_data.Code)
    res['industry'] = total_data['申万一级行业'].values

    return res


def settlement(ref_date, pos_df, bm, returns, type='risk_neutral'):
    inds = pos_df['industry']
    pos_df = pos_df[pos_df.columns.difference(['industry'])]

    ret_table = pos_df.sub(bm, axis=0).multiply(returns, axis=0)
    ret_aggregate = ret_table.groupby(inds).sum()
    ret_aggregate.loc['total', :] = ret_aggregate.sum().values
    ret_aggregate = ret_aggregate.stack()
    ret_aggregate.index.names = ['industry', 'portfolio']
    ret_aggregate.name = 'er'

    pos_table_with_returns = pos_df.sub(bm, axis=0)
    pos_table_with_returns['ret'] = returns
    ic_table = pos_table_with_returns.groupby(inds).corr()['ret']
    ic_table = ic_table.unstack(level=1)
    total_ic = pos_table_with_returns.corr()['ret']
    ic_table.loc['total', total_ic.index] = total_ic.values
    del ic_table['ret']
    ic_table = ic_table.stack()
    ic_table.index.names = ['industry', 'portfolio']
    ic_table.name = 'ic'

    res = pd.merge(ret_aggregate.reset_index(), ic_table.reset_index(), on=['industry', 'portfolio'])
    res['type'] = type
    res['Date'] = ref_date
    return res


def upload(ref_date, return_table, engine, table, build_type):
    engine.execute("delete from {1} where Date = '{0}' and type='{2}'".format(ref_date, table, build_type))
    return_table.to_sql(table, engine, if_exists='append', index=False)


def create_ond_day_pos(query_date, engine, big_universe=False, risk_neutral=True):
    industry_codes = get_industry_codes(query_date, engine)
    risk_cols, risk_factors = get_risk_factors(query_date, engine)
    index_components = get_index_components(query_date, engine)
    daily_returns = get_security_returns(query_date, engine)

    if big_universe:
        total_factors = get_all_the_factors(query_date, engine)
    else:
        total_factors = get_all_the_factors(query_date, engine, index_components.Code.tolist())

    factor_cols, total_data = merge_data(total_factors, industry_codes, risk_factors, index_components, daily_returns)
    processed_values = process_data(total_data, factor_cols, risk_cols)
    total_data[factor_cols] = processed_values
    
    if risk_neutral:
        pos_df = build_portfolio(processed_values, total_data, factor_cols, risk_cols, -0.01, 0.01)
    else:
        pos_df = build_portfolio(processed_values, total_data, factor_cols, risk_cols, -1., 1.)
    return pos_df, total_data


def update_factor_performance(ds, **kwargs):
    ref_date = kwargs['next_execution_date']
    if not isBizDay('china.sse', ref_date):
        logger.info("{0} is not a business day".format(ref_date))
        return 0

    ref_date = advanceDateByCalendar('china.sse', ref_date, '-2b')
    ref_date = ref_date.strftime('%Y-%m-%d')
    previous_date = advanceDateByCalendar('china.sse', ref_date, '-1b')

    this_day_pos, total_data = create_ond_day_pos(ref_date, source_db)
    last_day_pos, _ = create_ond_day_pos(previous_date, source_db)

    return_table = settlement(ref_date, this_day_pos, total_data['hs300'].values, total_data['D1LogReturn'].values)

    pos_diff_dict = {}

    for name in this_day_pos.columns.difference(['industry']):
        for ind in this_day_pos.industry.unique():
            pos_series = this_day_pos.loc[this_day_pos.industry == ind, name]
            if name in last_day_pos:
                last_series = last_day_pos.loc[last_day_pos.industry == ind, name]
                pos_diff = pos_series.sub(last_series, fill_value=0)
            else:
                pos_diff = pos_series
            pos_diff_dict[(name, ind)] = pos_diff.abs().sum()

        pos_series = this_day_pos[name]
        if name in last_day_pos:
            last_series = last_day_pos[name]
            pos_diff = pos_series.sub(last_series, fill_value=0)
        else:
            pos_diff = pos_series
        pos_diff_dict[(name, 'total')] = pos_diff.abs().sum()

    pos_diff_series = pd.Series(pos_diff_dict, name='turn_over')
    pos_diff_series.index.names = ['portfolio', 'industry']
    pos_diff_series = pos_diff_series.reset_index()

    return_table = pd.merge(return_table, pos_diff_series, on=['portfolio', 'industry'])
    upload(ref_date, return_table, destination_db, 'performance_uqer_300', 'risk_neutral')


def update_factor_performance_top_player(ds, **kwargs):
    ref_date = kwargs['next_execution_date']
    if not isBizDay('china.sse', ref_date):
        logger.info("{0} is not a business day".format(ref_date))
        return 0

    ref_date = advanceDateByCalendar('china.sse', ref_date, '-2b')
    ref_date = ref_date.strftime('%Y-%m-%d')
    previous_date = advanceDateByCalendar('china.sse', ref_date, '-1b')

    this_day_pos, total_data = create_ond_day_pos(ref_date, source_db, risk_neutral=False)
    last_day_pos, _ = create_ond_day_pos(previous_date, source_db, risk_neutral=False)

    return_table = settlement(ref_date, this_day_pos, total_data['hs300'].values, total_data['D1LogReturn'].values, type='top_player')

    pos_diff_dict = {}

    for name in this_day_pos.columns.difference(['industry']):
        for ind in this_day_pos.industry.unique():
            pos_series = this_day_pos.loc[this_day_pos.industry == ind, name]
            if name in last_day_pos:
                last_series = last_day_pos.loc[last_day_pos.industry == ind, name]
                pos_diff = pos_series.sub(last_series, fill_value=0)
            else:
                pos_diff = pos_series
            pos_diff_dict[(name, ind)] = pos_diff.abs().sum()

        pos_series = this_day_pos[name]
        if name in last_day_pos:
            last_series = last_day_pos[name]
            pos_diff = pos_series.sub(last_series, fill_value=0)
        else:
            pos_diff = pos_series
        pos_diff_dict[(name, 'total')] = pos_diff.abs().sum()

    pos_diff_series = pd.Series(pos_diff_dict, name='turn_over')
    pos_diff_series.index.names = ['portfolio', 'industry']
    pos_diff_series = pos_diff_series.reset_index()

    return_table = pd.merge(return_table, pos_diff_series, on=['portfolio', 'industry'])
    upload(ref_date, return_table, destination_db, 'performance_uqer_300', 'top_player')


def update_factor_performance_big_universe(ds, **kwargs):
    ref_date = kwargs['next_execution_date']
    if not isBizDay('china.sse', ref_date):
        logger.info("{0} is not a business day".format(ref_date))
        return 0

    ref_date = advanceDateByCalendar('china.sse', ref_date, '-2b')
    ref_date = ref_date.strftime('%Y-%m-%d')
    previous_date = advanceDateByCalendar('china.sse', ref_date, '-1b')

    this_day_pos, total_data = create_ond_day_pos(ref_date, source_db, big_universe=True)
    last_day_pos, _ = create_ond_day_pos(previous_date, source_db, big_universe=True)

    return_table = settlement(ref_date, this_day_pos, total_data['hs300'].values, total_data['D1LogReturn'].values)

    pos_diff_dict = {}

    for name in this_day_pos.columns.difference(['industry']):
        for ind in this_day_pos.industry.unique():
            pos_series = this_day_pos.loc[this_day_pos.industry == ind, name]
            if name in last_day_pos:
                last_series = last_day_pos.loc[last_day_pos.industry == ind, name]
                pos_diff = pos_series.sub(last_series, fill_value=0)
            else:
                pos_diff = pos_series
            pos_diff_dict[(name, ind)] = pos_diff.abs().sum()

        pos_series = this_day_pos[name]
        if name in last_day_pos:
            last_series = last_day_pos[name]
            pos_diff = pos_series.sub(last_series, fill_value=0)
        else:
            pos_diff = pos_series
        pos_diff_dict[(name, 'total')] = pos_diff.abs().sum()

    pos_diff_series = pd.Series(pos_diff_dict, name='turn_over')
    pos_diff_series.index.names = ['portfolio', 'industry']
    pos_diff_series = pos_diff_series.reset_index()

    return_table = pd.merge(return_table, pos_diff_series, on=['portfolio', 'industry'])
    upload(ref_date, return_table, destination_db, 'performance_big_universe_uqer_300', 'risk_neutral')


def update_factor_performance_big_universe_top_player(ds, **kwargs):
    ref_date = kwargs['next_execution_date']
    if not isBizDay('china.sse', ref_date):
        logger.info("{0} is not a business day".format(ref_date))
        return 0

    ref_date = advanceDateByCalendar('china.sse', ref_date, '-2b')
    ref_date = ref_date.strftime('%Y-%m-%d')
    previous_date = advanceDateByCalendar('china.sse', ref_date, '-1b')

    this_day_pos, total_data = create_ond_day_pos(ref_date, source_db, big_universe=True, risk_neutral=False)
    last_day_pos, _ = create_ond_day_pos(previous_date, source_db, big_universe=True, risk_neutral=False)

    return_table = settlement(ref_date, this_day_pos, total_data['hs300'].values, total_data['D1LogReturn'].values, type='top_player')

    pos_diff_dict = {}

    for name in this_day_pos.columns.difference(['industry']):
        for ind in this_day_pos.industry.unique():
            pos_series = this_day_pos.loc[this_day_pos.industry == ind, name]
            if name in last_day_pos:
                last_series = last_day_pos.loc[last_day_pos.industry == ind, name]
                pos_diff = pos_series.sub(last_series, fill_value=0)
            else:
                pos_diff = pos_series
            pos_diff_dict[(name, ind)] = pos_diff.abs().sum()

        pos_series = this_day_pos[name]
        if name in last_day_pos:
            last_series = last_day_pos[name]
            pos_diff = pos_series.sub(last_series, fill_value=0)
        else:
            pos_diff = pos_series
        pos_diff_dict[(name, 'total')] = pos_diff.abs().sum()

    pos_diff_series = pd.Series(pos_diff_dict, name='turn_over')
    pos_diff_series.index.names = ['portfolio', 'industry']
    pos_diff_series = pos_diff_series.reset_index()

    return_table = pd.merge(return_table, pos_diff_series, on=['portfolio', 'industry'])
    upload(ref_date, return_table, destination_db, 'performance_big_universe_uqer_300', 'top_player')


run_this1 = PythonOperator(
    task_id='update_factor_performance',
    provide_context=True,
    python_callable=update_factor_performance,
    dag=dag
)


run_this2 = PythonOperator(
    task_id='update_factor_performance_big_universe',
    provide_context=True,
    python_callable=update_factor_performance_big_universe,
    dag=dag
)


run_this3 = PythonOperator(
    task_id='update_factor_performance_top_player',
    provide_context=True,
    python_callable=update_factor_performance_top_player,
    dag=dag
)


run_this4 = PythonOperator(
    task_id='update_factor_performance_big_universe_top_player',
    provide_context=True,
    python_callable=update_factor_performance_big_universe_top_player,
    dag=dag
)


if __name__ == '__main__':
    update_factor_performance(None, next_execution_date=dt.datetime(2017, 1, 6))
