# -*- coding:utf-8 -*-
"""
author: Luke
datettime: 2020/7/3 7:47
"""

"使用tushare pro 接口获取数据"

from time import sleep

import numpy as np
import pandas as pd
import tushare as ts
from sqlalchemy import create_engine, CHAR

engine = create_engine('mysql+pymysql://root:00000@localhost:3306/stocks')

pro = ts.pro_api()

# 查询当前所有正常上市交易的股票列表
df_stock_lists = pro.stock_basic(fields='ts_code,symbol,name,area,industry,list_date')
df_stock_lists.to_sql('stock_basic', engine, if_exists='replace')

num_stocks = len(df_stock_lists)
print(f"There are {num_stocks} stocks available!")


def process_fina_indicator_to_sql(df_new):
    df_new['code_date'] = df_new.ts_code + '_' + df_new.end_date
    df_new.set_index('code_date', inplace=True)
    df_new.to_sql('fina_indicator', engine, if_exists='append', index_label='code_date',
                  dtype={'code_date': CHAR(20), 'ts_code': CHAR(10), 'name': CHAR(20), 'ann_date': CHAR(8), 'end_date':
                      CHAR(8)})

    print("success!!")


def get_fina_indicator(start_date=None, end_date=None):
    """
    财务指标数据，https://tushare.pro/document/2?doc_id=79
    # ## 初次建表
    # engine.execute("drop table if exists fina_indicator ")
    # get_fina_indicator(start_date='20141201')

    # ## 增量全部更新一个报表期，指定需要更新的报表期
    # get_fina_indicator(end_date='20200331')

    # 增量更新多个报表期
    # get_fina_indicator(start_date='20191231', end_date='20200331')

    :param start_date:
    :param end_date:
    :return:
    """
    assert start_date or end_date

    if not start_date:
        start_date = end_date

    idx_start = 0
    n_batch = 50
    idx_end = n_batch
    while idx_end < num_stocks:
        print(f"Get data: {idx_start}--{idx_end}")
        cur_stocks = ','.join(df_stock_lists.ts_code[idx_start:idx_end])
        df = pro.fina_indicator(ts_code=cur_stocks, start_date=start_date, limit=10000)
        df_new = pd.merge(df_stock_lists.loc[:, ['ts_code', 'name']], df, on='ts_code')

        process_fina_indicator_to_sql(df_new)

        idx_start = idx_end
        idx_end += n_batch

        sleep(np.random.rand() * 2)

    print(f"Get data success for {start_date} to {end_date} ")

# ## 初次建表
# engine.execute("drop table if exists fina_indicator ")
# get_fina_indicator(start_date='20141201')
# print('success')


# ## 增量更新
# get_fina_indicator(end_date='20200331')


## 补充数据


##主营业务构成 https://tushare.pro/document/2?doc_id=81
# fina_mainbz

# pro.fina_mainbz()
for i, ts_code in enumerate(df_stock_lists.ts_code):

    df = pro.fina_mainbz(ts_code=ts_code, type='P',start_date='20151231')

    # df_sum = df.groupby('end_date')['bz_sales'].sum()

    if i % 100 == 0:
        print(f"success for : {i}")

    break

print('done')
