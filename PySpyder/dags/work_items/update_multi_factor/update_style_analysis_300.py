# -*- coding: utf-8 -*-
"""
Created on 2017-6-21

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
from alphamind.analysis.factoranalysis import build_portfolio as bp


alpha_strategy = {}

logger = CustomLogger('MULTI_FACTOR', 'info')

start_date = dt.datetime(2012, 1, 1)
dag_name = 'update_style_analysis_300'

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

styles = [
    'BETA',
    'MOMENTUM',
    'SIZE',
    'EARNYILD',
    'RESVOL',
    'GROWTH',
    'BTOP',
    'LEVERAGE',
    'LIQUIDTY',
    'SIZENL',
]

source_db = sa.create_engine('mysql+mysqldb://sa:We051253524522@rm-bp1psdz5615icqc0y.mysql.rds.aliyuncs.com/multifactor?charset=utf8')
source_db2 = sa.create_engine('mysql+mysqldb://sa:We051253524522@rm-bp1psdz5615icqc0y.mysql.rds.aliyuncs.com/uqer?charset=utf8')
destination_db = sa.create_engine('mysql+mysqldb://sa:We051253524522@rm-bp1psdz5615icqc0y.mysql.rds.aliyuncs.com/factor_analysis?charset=utf8')


def get_industry_codes(ref_date, engine):
    return pd.read_sql("select Code, 申万一级行业 from factor_data where Date = '{0}'".format(ref_date), engine)


def get_risk_factors(ref_date, engine):
    risk_factor_list = ','.join(risk_factors_300)
    risk_factors = pd.read_sql(
        "select Code, {0} from risk_factor_300 where Date = '{1}'".format(risk_factor_list, ref_date), engine)
    risk_factors['Market'] = 1.
    risk_cols = risk_factors_300[:-1] + ['Market']
    return risk_cols, risk_factors


def get_security_returns(ref_date, engine):
    return pd.read_sql("select Code, D1LogReturn, isTradable from return_300 where Date = '{0}'".format(ref_date),
                       engine)


def get_index_components(ref_date, engine):
    df = pd.read_sql(
        "select Code, 300Weight as bm from index_components where Date = '{0}' and 300Weight > 0".format(ref_date), engine)
    df['bm'] /= 100.
    return df[['Code', 'bm']]


def get_all_the_factors(ref_date, engine, codes=None):
    if codes:
        codes_list = ','.join([str(c) for c in codes])
    else:
        codes_list = None
    if codes_list:

        uqer_styles = pd.read_sql(
            "select Code, {2} from risk_exposure where Date = '{0}' and Code in ({1})".format(ref_date,
                                                                                            codes_list,
                                                                                            ','.join(styles)),
            source_db2)
    else:
        uqer_styles = pd.read_sql(
            "select Code, {1} from risk_exposure where Date = '{0}'".format(ref_date,
                                                                          ','.join(styles)),
            source_db2)

    total_factors = uqer_styles

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

    if len(total_data) < 300:
        logger.warning('Data is missing for some codes')

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


def build_portfolio(er_values, total_data, factor_cols, risk_cols, risk_neutral=True):
    if risk_neutral:
        bm = total_data['bm'].values
        lbound = np.zeros(len(bm))
        ubound = 0.01 + bm
        risk_exposure = total_data[risk_cols].values
        lbound_exposure = bm @ risk_exposure
        ubound_exposure = bm @ risk_exposure

        is_trading = total_data['isTradable'].values
        ubound[~is_trading] = 0.

        factor_pos = {}

        for i, name in enumerate(factor_cols):
            er = er_values[:, i]
            weights = bp(er,
                         builder='linear',
                         risk_exposure=risk_exposure,
                         lbound=lbound,
                         ubound=ubound,
                         risk_target=(lbound_exposure, ubound_exposure),
                         solver='GLPK')
            factor_pos[name] = weights

            er = -er_values[:, i]
            weights = bp(er,
                         builder='linear',
                         risk_exposure=risk_exposure,
                         lbound=lbound,
                         ubound=ubound,
                         risk_target=(lbound_exposure, ubound_exposure),
                         solver='GLPK')
            factor_pos[name + '_neg'] = weights
    else:
        bm = total_data['bm'].values
        factor_pos = {}
        is_trading = total_data['isTradable'].values
        for i, name in enumerate(factor_cols):
            er = er_values[:, i]
            er[~is_trading] = np.min(er) - 9.
            weights = bp(er,
                         builder='rank',
                         use_rank=60) / 60. * bm.sum()
            factor_pos[name] = weights

            er = -er_values[:, i]
            er[~is_trading] = np.min(er) - 9.
            weights = bp(er,
                         builder='rank',
                         use_rank=60) / 60. * bm.sum()
            factor_pos[name + '_neg'] = weights

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


def upload(ref_date, return_table, engine, table):
    build_type = return_table['type'].unique()[0]
    universe = return_table['universe'].unique()[0]
    source = return_table['source'].unique()[0]

    engine.execute("delete from {1} where Date = '{0}' and type = '{2}' and universe = '{3}' and source = '{4}'"
                   .format(ref_date,
                           table,
                           build_type,
                           universe,
                           source))
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

    pos_df = build_portfolio(processed_values, total_data, factor_cols, risk_cols, risk_neutral=risk_neutral)
    return pos_df, total_data


def update_style_performance(ds, **kwargs):
    ref_date = kwargs['next_execution_date']
    if not isBizDay('china.sse', ref_date):
        logger.info("{0} is not a business day".format(ref_date))
        return 0

    ref_date = advanceDateByCalendar('china.sse', ref_date, '-2b')
    ref_date = ref_date.strftime('%Y-%m-%d')
    previous_date = advanceDateByCalendar('china.sse', ref_date, '-1b')

    this_day_pos, total_data = create_ond_day_pos(ref_date, source_db)
    last_day_pos, _ = create_ond_day_pos(previous_date, source_db)

    return_table = settlement(ref_date, this_day_pos, total_data['bm'].values, total_data['D1LogReturn'].values)

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
    return_table['source'] = 'style'
    return_table['universe'] = 'hs300'
    upload(ref_date, return_table, destination_db, 'performance')


def update_style_performance_top_100(ds, **kwargs):
    ref_date = kwargs['next_execution_date']
    if not isBizDay('china.sse', ref_date):
        logger.info("{0} is not a business day".format(ref_date))
        return 0

    ref_date = advanceDateByCalendar('china.sse', ref_date, '-2b')
    ref_date = ref_date.strftime('%Y-%m-%d')
    previous_date = advanceDateByCalendar('china.sse', ref_date, '-1b')

    this_day_pos, total_data = create_ond_day_pos(ref_date, source_db, risk_neutral=False)
    last_day_pos, _ = create_ond_day_pos(previous_date, source_db, risk_neutral=False)

    return_table = settlement(ref_date, this_day_pos, total_data['bm'].values, total_data['D1LogReturn'].values,
                              type='top_100')

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
    return_table['source'] = 'style'
    return_table['universe'] = 'hs300'
    upload(ref_date, return_table, destination_db, 'performance')


def update_style_performance_big_universe(ds, **kwargs):
    ref_date = kwargs['next_execution_date']
    if not isBizDay('china.sse', ref_date):
        logger.info("{0} is not a business day".format(ref_date))
        return 0

    ref_date = advanceDateByCalendar('china.sse', ref_date, '-2b')
    ref_date = ref_date.strftime('%Y-%m-%d')
    previous_date = advanceDateByCalendar('china.sse', ref_date, '-1b')

    this_day_pos, total_data = create_ond_day_pos(ref_date, source_db, big_universe=True)
    last_day_pos, _ = create_ond_day_pos(previous_date, source_db, big_universe=True)

    return_table = settlement(ref_date, this_day_pos, total_data['bm'].values, total_data['D1LogReturn'].values)

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
    return_table['source'] = 'style'
    return_table['universe'] = 'hs300_expand'
    upload(ref_date, return_table, destination_db, 'performance')


def update_style_performance_big_universe_top_100(ds, **kwargs):
    ref_date = kwargs['next_execution_date']
    if not isBizDay('china.sse', ref_date):
        logger.info("{0} is not a business day".format(ref_date))
        return 0

    ref_date = advanceDateByCalendar('china.sse', ref_date, '-2b')
    ref_date = ref_date.strftime('%Y-%m-%d')
    previous_date = advanceDateByCalendar('china.sse', ref_date, '-1b')

    this_day_pos, total_data = create_ond_day_pos(ref_date, source_db, big_universe=True, risk_neutral=False)
    last_day_pos, _ = create_ond_day_pos(previous_date, source_db, big_universe=True, risk_neutral=False)

    return_table = settlement(ref_date, this_day_pos, total_data['bm'].values, total_data['D1LogReturn'].values,
                              type='top_100')

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
    return_table['source'] = 'style'
    return_table['universe'] = 'hs300_expand'
    upload(ref_date, return_table, destination_db, 'performance')


run_this1 = PythonOperator(
    task_id='update_style_performance',
    provide_context=True,
    python_callable=update_style_performance,
    dag=dag
)

run_this2 = PythonOperator(
    task_id='update_style_performance_big_universe',
    provide_context=True,
    python_callable=update_style_performance_big_universe,
    dag=dag
)

run_this3 = PythonOperator(
    task_id='update_style_performance_top_100',
    provide_context=True,
    python_callable=update_style_performance_top_100,
    dag=dag
)

run_this4 = PythonOperator(
    task_id='update_style_performance_big_universe_top_100',
    provide_context=True,
    python_callable=update_style_performance_big_universe_top_100,
    dag=dag
)

if __name__ == '__main__':
    update_style_performance_big_universe_top_100(None, next_execution_date=dt.datetime(2017, 1, 6))
