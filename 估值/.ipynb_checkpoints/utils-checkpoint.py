# -*- coding:utf-8 -*-
"""
author: Luke
datettime: 2020/6/12 0:23
"""

import tushare as ts
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, VARCHAR, CHAR

engine = create_engine('mysql+pymysql://root:00000@localhost:3306/stocks')

UNIT = 1000000  # 百万



def df_process(df, table, code):
    df = df.drop(0, axis=0)
    df.index = df.iloc[:, 0]
    df = df.drop('报表日期', axis=1).T
    df = df.astype(float) / UNIT

    df['code'] = code
    df['date'] = df.index

    df = df[df.index>'2015']

    df['code_date'] = code + '_' + df.index

    df.reset_index(drop=True, inplace=True)

    df.to_sql(table, engine, if_exists='append',
              dtype={'code_date': CHAR(15), 'code': CHAR(6), 'date': CHAR(8), })
    return df


def from_sql(code, date, table):
    try:
        df = pd.read_sql(
            f'''select * from {table} 
                where code_date = "{code}_{date}"
            ''',
            engine)
        flag = True
    except:
        df = None
        flag = False
    return df, flag


def get_data(code, date):
    df_profit_statement, flag_profit_statement = from_sql(code, date, 'profit_statement')
    if not flag_profit_statement or len(df_profit_statement) < 1:
        df_profit_statement = ts.get_profit_statement(code)
        df_profit_statement = df_process(df_profit_statement, 'profit_statement', code)

    df_balance_sheet, flag_balance_sheet = from_sql(code, date, 'balance_sheet')
    if not flag_balance_sheet or len(df_balance_sheet) < 1:
        df_balance_sheet = ts.get_balance_sheet(code)
        df_balance_sheet = df_process(df_balance_sheet, 'balance_sheet', code)

    df_cash_flow, flag_cash_flow = from_sql(code, date, 'cash_flow')
    if not flag_cash_flow or len(df_cash_flow) < 1:
        df_cash_flow = ts.get_cash_flow(code)
        df_cash_flow = df_process(df_cash_flow, 'cash_flow', code)

        ts.get_

    return df_profit_statement, df_balance_sheet, df_cash_flow


if __name__ == '__main__':
    df_profit_statement, df_balance_sheet, df_cash_flow = get_data('002304', '2019-12-31')
    print('done')