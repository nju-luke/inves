# import tushare as ts
# df = ts.get_profit_statement('603195')
# df.index = df['报表日期']
# df.drop('报表日期',axis=1,inplace=True)
# df.drop('单位',inplace=True)
# df = df.astype(float)/1e6
# print(df)
from time import sleep

import numpy as np
import pandas as pd
import tushare as ts
from sqlalchemy import create_engine, CHAR

engine = create_engine('mysql+pymysql://root:00000@localhost:3306/stocks')


pro = ts.pro_api()

#查询当前所有正常上市交易的股票列表
df_stock_lists = pro.stock_basic(fields='ts_code,symbol,name,area,industry,list_date')
df_stock_lists.to_sql('stock_basic',engine,if_exists='replace')

num_stocks = len(df_stock_lists)
print(f"There are {num_stocks} stocks available!")


def process_to_sql(df_new):
    df_new['code_date'] = df_new.ts_code + '_' + df_new.end_date
    df_new.set_index('code_date', inplace=True)
    df_new.to_sql('fina_indicator', engine, if_exists='append', index_label='code_date',
                  dtype={'code_date': CHAR(20), 'ts_code': CHAR(10), 'name': CHAR(20),'ann_date': CHAR(8),'end_date':
                      CHAR(8)})

    print("success!!")




def get_fina_indicator(start_date=None, end_date=None):
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

        process_to_sql(df_new)

        idx_start = idx_end
        idx_end += n_batch

        sleep(np.random.rand()*2)



## 初次建表
engine.execute("drop table if exists fina_indicator ")
get_fina_indicator(start_date='20141201')
print('success')


# ## 增量更新
# engine.execute("drop table if exists fina_indicator ")
# get_fina_indicator(end_date='20200331')
# print('success')


## 补充数据


