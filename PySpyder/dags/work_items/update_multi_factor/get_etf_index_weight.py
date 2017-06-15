# -*- coding: utf-8 -*-
u"""
Created on 06/15/2017

@author: yucheng.lai
"""
import requests
import datetime as dt
import pandas as pd
from sqlalchemy import create_engine
from bs4 import BeautifulSoup
import uqer
from PyFin.api import isBizDay
from PyFin.api import advanceDateByCalendar


industry_mapping = {
    '农林牧渔': 'Agriculture',
    '采掘': 'ExtractiveIndustry',
    '化工': 'ChemicalIndustry',
    '钢铁': 'Metal',
    '有色金属': 'DiversifiedMetal',
    '机械设备': 'Machinary',
    '电子': 'ElectronicIndustry',
    '家用电器': 'HouseholdAppliances',
    '食品饮料': 'FoodAndBeverage',
    '纺织服装': 'TextileAndGarment',
    '轻工制造': 'LightManufacturing',
    '医药生物': 'MedicationAndBio',
    '公用事业': 'PublicUtility',
    '交通运输': 'CommunicationsAndTransportation',
    '房地产': 'RealEstate',
    '商业贸易': 'CommercialTrade',
    '休闲服务': 'LeisureServices',
    '综合': 'Synthetics',
    '建筑材料': 'ConstructionAndMaterial',
    '建筑装饰': 'BuildingDecoration',
    '电气设备': 'ElectricalEquip',
    '国防军工': 'DefenseIndustry',
    '汽车': 'MotorVehicle',
    '计算机': 'Computer',
    '传媒': 'MultiMedia',
    '通信': 'Telecoms',
    '银行': 'Bank',
    '非银金融': 'NonBankFinancial',
}

ind_col = [
    'CommunicationsAndTransportation',
    'LeisureServices',
    'MultiMedia',
    'PublicUtility',
    'Agriculture',
    'ChemicalIndustry',
    'MedicationAndBio',
    'CommercialTrade',
    'DefenseIndustry',
    'HouseholdAppliances',
    'ConstructionAndMaterial',
    'BuildingDecoration',
    'RealEstate',
    'DiversifiedMetal',
    'Machinary',
    'MotorVehicle',
    'ElectronicIndustry',
    'ElectricalEquip',
    'TextileAndGarment',
    'Synthetics',
    'Computer',
    'LightManufacturing',
    'Telecoms',
    'ExtractiveIndustry',
    'Metal',
    'Bank',
    'NonBankFinancial',
    'FoodAndBeverage',
]


def get_nffund_idx_etf_component(date, index):
    date = dt.datetime.strptime(date, '%Y%m%d')
    if not isBizDay('China.SSE', date):
        date = advanceDateByCalendar('China.SSE', date, '-1b')
    pre_trading_date = advanceDateByCalendar('China.SSE', date, '-1b').strftime('%Y%m%d')
    if index == 'zz500':
        date = date.strftime('%Y%m%d')
        url = "http://www.nffund.com/etf/bulletin/ETF500/510500{date}.txt".format(date=date)
        html_text = requests.get(url)._content.decode('gbk').split('TAGTAG\r')[1]
        res = []
        col_name = ['Code', 'drop', 'Volume', 'drop', 'drop', 'drop', 'drop']
        for line in html_text.split('\r'):
            res.append(line.replace(' ', '').replace('\n','').split('|'))
        res = pd.DataFrame(res, columns=col_name)
        res = res.drop('drop', axis=1).iloc[:500]
    elif index == 'hs300':
        url = "http://www.huatai-pb.com/etf-web/etf/index?fundcode=510300&beginDate={date}".format(date=date.strftime('%Y-%m-%d'))
        html_text = requests.get(url)._content.decode('utf8')
        soup = BeautifulSoup(html_text, "lxml")
        res = []
        for item in soup.find_all('tr', {'align': 'center'})[1:]:
            sub_item = item.find_all('td')
            res.append([sub_item[0].text, sub_item[2].text])
        col_name = ['Code', 'Volume']
        res = pd.DataFrame(res, columns=col_name)
    elif index == 'sz50':
        url = "http://fund.chinaamc.com/product/fundShengoushuhuiqingdan.do"
        html_text = requests.post(url, data={'querryDate': date.strftime('%Y-%m-%d'), 'fundcode': '510050'})\
                    ._content.decode('utf8')
        soup = BeautifulSoup(html_text, "lxml")
        res = []
        for item in soup.find_all('tr', '')[17:]:
            sub_item = item.find_all('td')
            res.append([sub_item[0].text, sub_item[2].text])
        col_name = ['Code', 'Volume']
        res = pd.DataFrame(res, columns=col_name)
    else:
        raise KeyError('Do not have source for index %s yet...' % index)

    # convert string code to int code
    res['Code'] = res['Code'].apply(int)

    # fetch eod close price
    engine = create_engine('mssql+pymssql://sa:A12345678!@10.63.6.219/MultiFactor?charset=utf8')
    sql = 'select [Code], [Close] as PreClose from TradingInfo1 where Date = %s' % pre_trading_date
    close_data = pd.read_sql(sql, engine)
    res = res.merge(close_data, on='Code', how='left')

    # calculate weight
    res['weight'] = res['PreClose'].apply(float) * res['Volume'].apply(float)
    res['weight'] = res['weight'] / res['weight'].sum()
    res = res[['Code', 'weight']]
    return res


def get_sw_industry_weight(industry_component):
    client = uqer.Client(username='13817268186', password='we083826')
    ticker = list(industry_component['Code'].apply(lambda x: str(x).zfill(6)))
    df = uqer.DataAPI.EquIndustryGet(industryVersionCD="010303",
                                     ticker=ticker,
                                     field="ticker,industryName1",
                                     pandas="1")
    df.rename(columns={'ticker': 'Code',
                       'industryName1': 'industry'},
              inplace=True)
    df['Code'] = df['Code'].apply(int)
    df = industry_component.merge(df, on='Code', how='left')
    res = df.groupby('industry')['weight'].sum().reset_index()
    res['industry'] = res['industry'].apply(lambda x: industry_mapping[x])
    res = res.merge(pd.DataFrame(ind_col, columns=['industry']), on='industry', how='outer').fillna(0)
    res = res.set_index('industry').loc[ind_col].reset_index()
    return res


if __name__ == "__main__":
    date = '20170614'
    index = 'zz500'
    start = dt.datetime.now()
    res = get_nffund_idx_etf_component(date=date, index=index)
    print(res)

    res = get_sw_industry_weight(res)

    print(res)
    print("elapse time is %s" % (dt.datetime.now() - start))
