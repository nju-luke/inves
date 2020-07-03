# -*- coding:utf-8 -*-
"""
author: Luke
datettime: 2020/7/3 7:47
"""
from sqlalchemy.exc import ProgrammingError

"使用tushare pro 接口获取数据"

from time import sleep

import numpy as np
import pandas as pd
import tushare as ts
from sqlalchemy import create_engine, VARCHAR

engine = create_engine('mysql+pymysql://root:00000@localhost:3306/stocks')

pro = ts.pro_api()

# 查询当前所有正常上市交易的股票列表
df_stock_lists = pro.stock_basic(fields='ts_code,symbol,name,area,industry,list_date')
df_stock_lists.to_sql('stock_basic', engine, if_exists='replace',
                      dtype=dict((c, VARCHAR(100)) for c in df_stock_lists.columns)
                      )

num_stocks = len(df_stock_lists)
print(f"There are {num_stocks} stocks available!")


def process_fina_indicator_to_sql(df_new):
    df_new['code_date'] = df_new.ts_code + '_' + df_new.end_date
    df_new.set_index('code_date', inplace=True)
    df_new.to_sql('fina_indicator', engine, if_exists='append', index_label='code_date',
                  dtype={'code_date': VARCHAR(20), 'ts_code': VARCHAR(10), 'name': VARCHAR(20), 'ann_date': VARCHAR(8),
                         'end_date':
                             VARCHAR(8)})

    print("success!!")
    return df_new


def download_fina_indicator_all(start_date=None, end_date=None):
    """
    财务指标数据，https://tushare.pro/document/2?doc_id=79
    # ## 初次建表
    # engine.execute("drop table if exists fina_indicator ")
    # download_fina_indicator_all(start_date='20141201')

    # ## 增量全部更新一个报表期，指定需要更新的报表期
    # download_fina_indicator_all(end_date='20200331')

    # 增量更新多个报表期
    # download_fina_indicator_all(start_date='20191231', end_date='20200331')

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


def get_fina_indicator(ts_code, start_date='20141201'):
    """
    财务指标数据
    :param ts_code:
    :param start_date:
    :return:
    """
    df = pd.read_sql(f'select * from fina_indicator where ts_code="{ts_code}" ', engine)
    if len(df) > 0:
        return df

    df = pro.fina_indicator(ts_code=ts_code, start_date=start_date, limit=10000)
    df_new = pd.merge(df_stock_lists[df_stock_lists.ts_code == ts_code].loc[:, ['ts_code', 'name']], df, on='ts_code')
    df_new = process_fina_indicator_to_sql(df_new)

    return df_new


def get_fina_mainbz(ts_code):
    ##主营业务构成 https://tushare.pro/document/2?doc_id=81

    df = pd.read_sql(f'select * from fina_mainbz where ts_code="{ts_code}" ', engine)
    if len(df) > 0:
        return df

    df = pro.fina_mainbz(ts_code=ts_code, type='P', start_date='20151231')

    ## 删除其中重复的项
    try:
        df = df.assign(rnk=df.groupby(['end_date', 'bz_item'])['bz_cost'].rank(method='first', ascending=False))
        df = df[(df.rnk < 2) | df.rnk.isna()].drop('rnk', axis=1)

        ## 计算比率
        df1 = df.groupby(['end_date', 'bz_item']).sum().groupby(level=0).apply(
            lambda x: 100 * x / x.sum()).reset_index()
        df_new = pd.merge(df, df1, on=['end_date', 'bz_item'], suffixes=('', '_ratio'))
    except:
        df_new = df

    ## 加到数据库中
    df_new['code_date'] = df_new.ts_code + '_' + df_new.end_date
    df_new.set_index('code_date', inplace=True)
    df_new.to_sql('fina_mainbz', engine, if_exists='append', index_label='code_date',
                  dtype={'code_date': VARCHAR(20), 'ts_code': VARCHAR(10), 'biz_item': VARCHAR(100), 'end_date':
                      VARCHAR(8)})

    return df_new


def download_fina_mainbz_all():
    ## 建立主营业务表
    for i, ts_code in enumerate(df_stock_lists.ts_code):
        df = get_fina_mainbz(ts_code)

        if i % 100 == 0:
            print(f"success for : {i+1}")

        if i > 10:
            break
    print("Dowanload all fina mainbz done!")


def supp_daily_basic(ts_codes=None, start_date='20160101'):
    """
    每日行情数据
    :param ts_codes: 列表或者set
    :param start_date:
    """
    if not ts_codes:
        ts_codes = set(df_stock_lists.ts_code)
        print("Set ts_codes to all stock codes.")
    for i, ts_code in enumerate(ts_codes):
        try:
            df_exists = pd.read_sql(f"select trade_date from daily_basic where ts_code='{ts_code}' "
                                    f"order by trade_date desc limit 1", engine)
            if len(df_exists) > 0 and start_date < df_exists.iloc[0, 0]:
                start_date = df_exists.iloc[0, 0]
        except ProgrammingError:
            pass

        df = pro.daily_basic(ts_code=ts_code, start_date=start_date)
        df['code_date'] = df.ts_code + '_' + df.trade_date
        df.set_index('code_date', inplace=True)
        df.to_sql('daily_basic', engine, if_exists='append', index_label='code_date',
                  dtype={'code_date': VARCHAR(20), 'ts_code': VARCHAR(10), 'trade_date': VARCHAR(8)}
                  )
        sleep(np.random.rand())
        if i%100 == 0:
            print(f"Success num: f{i+1}")

    print(f"Get data from f{start_date} success.")





if __name__ == '__main__':
    # ## 初次建表 财务指标
    # engine.execute("drop table if exists fina_indicator ")
    # download_fina_indicator_all(start_date='20141201')

    # df = get_fina_indicator('000001.SZ')

    ## 全量主营业务
    # download_fina_mainbz_all()

    ## 每日指标
    supp_daily_basic()
    print('done')
