# -*- coding:utf-8 -*-
"""
author: byangg
datettime: 7/9/2020 10:21
"""

from sqlalchemy import create_engine, VARCHAR
import tushare as ts
import pandas as pd

engine = create_engine('mysql+pymysql://root:00000@localhost:3306/stocks')

KEY = "ts_code"


def to_mysql(df, table_name, if_exists, key=KEY, dtype=None):
    df.set_index(key, drop=True, inplace=True)
    if not dtype:
        dtype = {'ts_code': VARCHAR(10)}
    else:
        dtype.update({'ts_code': VARCHAR(10)})
    df.to_sql(table_name, engine, if_exists=if_exists,
              index_label='ts_code',
              dtype=dtype)

    print("save to mysql success!!")

pro = ts.pro_api()

def data_process(df, date_filter):
    df = df.copy()
#     df = df[df.end_date.apply(lambda x:(x.endswith('1231') or x > cur_year) and x>'2010')] # 保留当前年份报告期数据
    df = df[df.end_date.apply(date_filter)]
    df['end_date'] = df['end_date'].apply(lambda x:x[:4]+"-"+x[4:6]+"-"+x[6:])

    df['sumv'] = df.fillna(0).sum(axis=1)
    df = df.groupby(['ts_code','end_date']).apply(lambda t:t[t.sumv==t.sumv.max()]).reset_index(drop=True).groupby(['ts_code','end_date']).head(1)
    df.index = df['end_date']
    df = df.drop('end_date', axis=1)
    return df


def get_data(ts_code, date_filter):
    balancesheet = pro.balancesheet(ts_code=ts_code)
    balancesheet = data_process(balancesheet,date_filter)

    income = pro.income(ts_code=ts_code)
    income = data_process(income,date_filter)

    cashflow = pro.cashflow(ts_code=ts_code)
    cashflow = data_process(cashflow,date_filter)

    fina_indicator = pro.fina_indicator(ts_code=ts_code)
    fina_indicator = data_process(fina_indicator,date_filter)

    df1 = pd.merge(balancesheet, income, on='end_date', suffixes=('', '_inc'))
    df1 = pd.merge(df1, cashflow, on='end_date', suffixes=('', '_cas'))
    df1 = pd.merge(df1, fina_indicator, on='end_date', suffixes=('', '_fina'))

    df1 = df1.fillna(0)
    return df1