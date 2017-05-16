# -*- coding: utf-8 -*-
"""
Created on 2017-5-15

@author: cheng.li
"""

import datetime as dt
import numpy as np
import sqlalchemy
import pandas as pd
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from alphamind.examples.config import risk_factors_500
from alphamind.data.standardize import standardize
from alphamind.data.neutralize import neutralize
from alphamind.data.winsorize import winsorize_normal
from alphamind.portfolio.linearbuilder import linear_build
from simpleutils import CustomLogger
from PyFin.api import isBizDay

logger = CustomLogger('MULTI_FACTOR', 'info')

start_date = dt.datetime(2017, 4, 1)
dag_name = 'update_daily_portfolio'

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


def update_daily_portfolio(ds, **kwargs):
    execution_date = kwargs['next_execution_date']

    if not isBizDay('china.sse', execution_date):
        logger.info("{0} is not a business day".format(execution_date))
        return 0

    ref_date = execution_date.strftime('%Y-%m-%d')

    common_factors = ['EPSAfterNonRecurring', 'DivP']
    prod_factors = ['CFinc1', 'BDTO', 'RVOL']

    factor_weights = 1. / np.array([15.44 * 2., 32.72 * 2., 49.90, 115.27, 97.76])
    factor_weights = factor_weights / factor_weights.sum()

    index_components = '500Weight'

    engine = sqlalchemy.create_engine('mysql+mysqldb://sa:we083826@10.63.6.176/multifactor?charset=utf8')

    common_factors_df = pd.read_sql("select Date, Code, 申万一级行业, {0} from factor_data where Date = '{1}'"
                                    .format(','.join(common_factors), ref_date), engine)

    prod_factors_df = pd.read_sql("select Date, Code, {0} from prod_500 where Date = '{1}'"
                                  .format(','.join(prod_factors), ref_date), engine)

    risk_factor_df = pd.read_sql("select Date, Code, {0} from risk_factor_500 where Date = '{1}'"
                                 .format(','.join(risk_factors_500), ref_date), engine)

    index_components_df = pd.read_sql("select Date, Code, {0} from index_components where Date = '{1}'"
                                      .format(index_components, ref_date), engine)

    total_data = pd.merge(common_factors_df, prod_factors_df, on=['Date', 'Code'])
    total_data = pd.merge(total_data, risk_factor_df, on=['Date', 'Code'])
    total_data = pd.merge(total_data, index_components_df, on=['Date', 'Code'])
    total_data = total_data[total_data[index_components] != 0]
    total_data[index_components] = total_data[index_components] / 100.0

    total_factors = common_factors + prod_factors
    risk_factors_names = risk_factors_500 + ['Market']
    total_data['Market'] = 1.

    all_factors = total_data[total_factors]
    risk_factors = total_data[risk_factors_names]

    factor_processed = neutralize(risk_factors.values,
                                  standardize(winsorize_normal(all_factors.values)))

    normed_factor = pd.DataFrame(factor_processed, columns=total_factors, index=total_data.Date)

    er = normed_factor @ factor_weights

    # portfolio construction

    bm = total_data[index_components].values
    lbound = np.zeros(len(total_data))
    ubound = 0.01 + bm
    lbound_exposure = -0.01
    ubound_exposure = 0.01
    risk_exposure = total_data[risk_factors_names].values

    # get black list 1
    engine = sqlalchemy.create_engine('mssql+pymssql://sa:A12345678!@10.63.6.100/WindDB')
    black_list = pd.read_sql("select S_INFO_WINDCODE, S_INFO_LISTDATE, sum(S_SHARE_RATIO) as s_ratio from ASHARECOMPRESTRICTED \
                              where S_INFO_LISTDATE BETWEEN '{0}' and '{1}' \
                              and S_SHARE_LSTTYPECODE=479002000 "
                             "GROUP BY S_INFO_WINDCODE, S_INFO_LISTDATE ORDER BY s_ratio DESC;"
                             .format((execution_date - dt.timedelta(days=7)).strftime('%Y%m%d'),
                                     (execution_date + dt.timedelta(days=14)).strftime('%Y%m%d')), engine)

    black_list = black_list[black_list['s_ratio'] >= 3.]
    black_list.S_INFO_WINDCODE = black_list.S_INFO_WINDCODE.str.split('.').apply(lambda x: int(x[0]))

    mask_array = total_data.Code.isin(black_list.S_INFO_WINDCODE)
    ubound[mask_array.values] = 0.

    # get black list 2
    black_list2 = pd.read_sql("select S_INFO_WINDCODE, AVG(S_WQ_AMOUNT) as avg_amount from ASHAREWEEKLYYIELD "
                              "where TRADE_DT < {1} and TRADE_DT >= {0} GROUP BY S_INFO_WINDCODE;"
                              .format((execution_date - dt.timedelta(days=30)).strftime('%Y%m%d'),
                                      execution_date.strftime('%Y%m%d')), engine)
    black_list2 = black_list2[black_list2['avg_amount'] <= 15000.]
    black_list2.S_INFO_WINDCODE = black_list2.S_INFO_WINDCODE.str.split('.').apply(lambda x: int(x[0]))

    mask_array2 = total_data.Code.isin(black_list2.S_INFO_WINDCODE)
    ubound[mask_array2.values] = 0.

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
        portfolio = pd.DataFrame({'weight': ret,
                                  'industry': total_data['申万一级行业'].values,
                                  'zz500': total_data[index_components].values}, index=total_data.Code)

        portfolio.to_csv('~/mnt/personal/licheng/portfolio/zz500/{0}.csv'.format(ref_date), encoding='gbk')

    return 0


run_this1 = PythonOperator(
    task_id='update_daily_portfolio',
    provide_context=True,
    python_callable=update_daily_portfolio,
    dag=dag
)


if __name__ == '__main__':
    update_daily_portfolio(None, next_execution_date=dt.datetime(2017, 5, 15))