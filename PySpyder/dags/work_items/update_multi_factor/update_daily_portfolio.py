# -*- coding: utf-8 -*-
"""
Created on 2017-5-15

@author: cheng.li
"""

import datetime as dt
import numpy as np
import sqlalchemy
import pandas as pd
import pymongo
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from alphamind.examples.config import risk_factors_500
from alphamind.data.standardize import standardize
from alphamind.data.neutralize import neutralize
from alphamind.data.winsorize import winsorize_normal
from alphamind.analysis.factoranalysis import build_portfolio
from simpleutils import CustomLogger
from PyFin.api import isBizDay
from PyFin.api import advanceDateByCalendar

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
    schedule_interval='0 8 * * 1,2,3,4,5'
)


def update_daily_portfolio(ds, **kwargs):
    execution_date = kwargs['next_execution_date']

    if not isBizDay('china.sse', execution_date):
        logger.info("{0} is not a business day".format(execution_date))
        return 0

    prev_date = advanceDateByCalendar('china.sse', execution_date, '-1b')

    logger.info("factor data is loading for {0}".format(prev_date))
    logger.info("Current running date is {0}".format(execution_date))

    common_factors = ['EPSAfterNonRecurring', 'DivP']
    prod_factors = ['CFinc1', 'BDTO', 'RVOL']

    factor_weights = 1. / np.array([15.44 * 2., 32.72 * 2., 49.90, 115.27, 97.76])
    factor_weights = factor_weights / factor_weights.sum()

    index_components = '500Weight'

    engine = sqlalchemy.create_engine('mysql+mysqldb://sa:we083826@10.63.6.176/multifactor?charset=utf8')

    common_factors_df = pd.read_sql("select Date, Code, 申万一级行业, {0} from factor_data where Date = '{1}'"
                                    .format(','.join(common_factors), prev_date), engine)

    prod_factors_df = pd.read_sql("select Date, Code, {0} from prod_500 where Date = '{1}'"
                                  .format(','.join(prod_factors), prev_date), engine)

    risk_factor_df = pd.read_sql("select Date, Code, {0} from risk_factor_500 where Date = '{1}'"
                                 .format(','.join(risk_factors_500), prev_date), engine)

    index_components_df = pd.read_sql("select Date, Code, {0} from index_components where Date = '{1}'"
                                      .format(index_components, prev_date), engine)

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
    risk_exposure = total_data[risk_factors_names].values
    risk_lbound = bm @ risk_exposure
    risk_ubound = bm @ risk_exposure

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

    # get black list 3
    black_list3 = pd.read_sql("SELECT S_INFO_WINDCODE, S_DQ_SUSPENDDATE FROM ASHARETRADINGSUSPENSION AS a "
                              "WHERE a.S_DQ_SUSPENDDATE = (SELECT top 1 S_DQ_SUSPENDDATE FROM ASHARETRADINGSUSPENSION AS b "
                              "WHERE a.S_INFO_WINDCODE=b.S_INFO_WINDCODE and cast(floor(cast(b.OPDATE as float)) as datetime) <= '{0}' ORDER BY b.S_DQ_SUSPENDDATE DESC) "
                              "AND a.S_INFO_WINDCODE IN (SELECT S_INFO_WINDCODE FROM ASHAREDESCRIPTION AS c "
                              "WHERE c.S_INFO_DELISTDATE IS NULL) AND (a.S_DQ_SUSPENDDATE>='{1}' OR (a.S_DQ_RESUMPDATE IS NULL AND a.S_DQ_SUSPENDTYPE=444003000))"
                              .format(execution_date, execution_date.strftime('%Y%m%d')),
                              engine)
    black_list3.S_INFO_WINDCODE = black_list3.S_INFO_WINDCODE.str.split('.').apply(lambda x: int(x[0]))
    mask_array3 = total_data.Code.isin(black_list3.S_INFO_WINDCODE)
    ubound[mask_array3.values] = 0.

    # manual black list
    try:
        bk_list = pd.read_csv('~/mnt/sharespace/personal/licheng/portfolio/zz500_black_list/{0}.csv'.format(
            prev_date.strftime('%Y-%m-%d')),
                              encoding='gbk',
                              names=['code'])
        logger.info('Manual black list exists for the date: {0}'.format(prev_date.strftime('%Y-%m-%d')))
        for code in bk_list['code']:
            ubound[total_data.Code == int(code)] = 0.
    except FileNotFoundError:
        logger.info('No manual black list exists for the date: {0}'.format(prev_date.strftime('%Y-%m-%d')))

    weights = build_portfolio(er,
                              builder='linear',
                              risk_exposure=risk_exposure,
                              lbound=lbound,
                              ubound=ubound,
                              risk_target=(risk_lbound, risk_ubound),
                              solver='GLPK')

    portfolio = pd.DataFrame({'weight': weights,
                              'industry': total_data['申万一级行业'].values,
                              'zz500': total_data[index_components].values,
                              'er': er}, index=total_data.Code)

    client = pymongo.MongoClient('mongodb://10.63.6.176:27017')
    db = client.multifactor
    portfolio_collection = db.portfolio

    detail_info = {}
    for code, w, bm_w, ind, r in zip(total_data.Code.values, weights, total_data[index_components].values,
                                     total_data['申万一级行业'].values, er):
        detail_info[str(code)] = {
            'weight': w,
            'industry': ind,
            'zz500': bm_w,
            'er': r
        }

    portfolio_dict = {'Date': prev_date,
                      'portfolio': detail_info}

    portfolio_collection.delete_many({'Date': prev_date})
    portfolio_collection.insert_one(portfolio_dict)

    portfolio.to_csv('~/mnt/sharespace/personal/licheng/portfolio/zz500/{0}.csv'.format(prev_date.strftime('%Y-%m-%d')),
                     encoding='gbk')

    return 0


run_this1 = PythonOperator(
    task_id='update_daily_portfolio',
    provide_context=True,
    python_callable=update_daily_portfolio,
    dag=dag
)

if __name__ == '__main__':
    update_daily_portfolio(None, next_execution_date=dt.datetime(2017, 5, 31))
