# -*- coding:utf-8 -*-
"""
author: byangg
datettime: 6/12/2020 09:46
"""

import tushare as ts
import pandas as pd
import sqlite3
import datetime

conn = sqlite3.connect('data')


def get_data(year, quarter, func, table_name, if_exists='append'):
    try:
        df = pd.read_sql(f'select * from {table_name} where year={year} and quarter={quarter}', conn)
        if len(df) <= 10:
            raise Exception
    except:
        df = func(year, quarter)
        df['year'] = year
        df['quarter'] = quarter
        df.to_sql(table_name, conn, if_exists='append')
    return df


func = ts.get_profit_data
# df = get_data(2019,4, func, 'profit_data')
# df1 = get_data(2018,4, func, 'profit_data')
# df2 = get_data(2017,4, func, 'profit_data')
# df3 = get_data(2016,4, func, 'profit_data')


def get_basic(date, table_name):
    date_ = datetime.datetime(year=int(date[:4]), month=int(date[5:7]), day=int(date[8:]))
    year = date_.year
    quarter = (date_.month - 1) // 3 + 1
    try:
        df = pd.read_sql(f'select * from {table_name} where year={year} and quarter={quarter}', conn)
        if len(df) <= 10:
            raise Exception
    except:
        flag = False
        while not flag:
            try:
                df = ts.get_stock_basics(date)
                print(f'get data success for {date}. -----')
                flag = True
            except:
                print(f'There is no data for {date}')
            date_ -= datetime.timedelta(days=1)
            date = date_.strftime("%Y-%m-%d")
        df = ts.get_stock_basics(date)
        df['year'] = year
        df['quarter'] = quarter
        df.to_sql(table_name, conn, if_exists='append')
    return df


df = get_basic('2016-12-31', 'stock_basics')

# ## today_all
# df = ts.get_today_all()
# df.to_sql('today_all', conn)


# ##
# def get_report_data(year, quarter):
#     # try:
#     #     df = pd.read_sql(f'select * from report_data where ')
#     df = ts.get_report_data(year, quarter)
#     df.to_sql('report_data', conn)
#     return df
#
# df = get_report_data(2019,4)


##


# def get_profit_data(year, quarter):
#     try:
#         df = pd.read_sql(f'select * from profit_data where year={year} and quarter={quarter}', conn)
#         if len(df) <=10:
#             raise Exception
#     except:
#         df = ts.get_profit_data(year, quarter)
#         df['year'] = year
#         df['quarter'] = quarter
#         df.to_sql('profit_data', conn, if_exists='append')
#     return df
#
# df = get_profit_data(2019,4)
# df = get_profit_data(2018,4)
# df = get_profit_data(2017,4)
# df = get_profit_data(2016,4)

# df1 = ts.get_sz50s()
# df2 = pd.concat([df.set_index('code'),df1.set_index('code')], axis=1,  join='inner')
#
# import numpy as np
# np.quantile()
#
#
#
# def get_growth_data(year, quarter):
#     try:
#         df = pd.read_sql(f'select * from growth_data where year={year} and quarter={quarter}', conn)
#         if len(df) <=10:
#             raise Exception
#     except:
#         df = ts.get_growth_data(year, quarter)
#         df['year'] = year
#         df['quarter'] = quarter
#         df.to_sql('growth_data', conn, if_exists='append')
#     return df
#
#
# df = get_growth_data(2019,4)
# df = get_growth_data(2018,4)
# df = get_growth_data(2017,4)
# df = get_growth_data(2016,4)
#
#


print('done')
