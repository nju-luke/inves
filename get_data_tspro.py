# -*- coding:utf-8 -*-
"""
author: Luke
datettime: 2020/7/3 7:47
"""
import time

from utils import to_mysql

# import pymysql

"使用tushare pro 接口获取数据"

from time import sleep
import datetime
import numpy as np
import pandas as pd
import tushare as ts
from sqlalchemy import create_engine

engine = create_engine('mysql+pymysql://root:00000@localhost:3306/stocks')
# conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='00000', db='stocks')
# cur = conn.cursor()

pro = ts.pro_api()

KEY = "ts_code"
DATE_FORMAT = "%Y%m%d"


def get_stock_list():
    try:
        # 查询当前所有正常上市交易的股票列表
        df_stock_lists = pro.stock_basic(fields='ts_code,symbol,name,area,industry,list_date')

        num_stocks = len(df_stock_lists)
        print(f"There are {num_stocks} stocks available!")

        to_mysql(df_stock_lists, "stock_basic", "replace")
    except:
        print("Data from website not available, use local data.")
        df_stock_lists = pd.read_sql("select * from stock_basic", engine)
    df_stock_lists.reset_index(inplace=True)
    return df_stock_lists

df_stock_lists = get_stock_list()
num_stocks = len(df_stock_lists)

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
    try:
        pd.read_sql('truncate table fina_indicator',engine)
    except:
        pass
    while idx_end < num_stocks:
        print(f"Get data: {idx_start}--{idx_end}")
        cur_stocks = ','.join(df_stock_lists.ts_code[idx_start:idx_end])
        df = pro.fina_indicator(ts_code=cur_stocks, start_date=start_date, limit=10000)
        df_new = pd.merge(df_stock_lists.loc[:, ['ts_code', 'name']], df, on='ts_code')

        to_mysql(df_new, "fina_indicator", "append")

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
    df_new = to_mysql(df_new, "fina_indicator", "append")

    return df_new


def get_fina_mainbz(ts_code):
    ##主营业务构成 https://tushare.pro/document/2?doc_id=81
    ## todo 一部分存在一部分不存在
    try:
        df = pd.read_sql(f'select * from fina_mainbz where ts_code="{ts_code}" ', engine)
        if len(df) > 0:
            return df
    except:
        pass

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

    to_mysql(df_new, "fina_mainbz", "append")

    return df_new


def download_fina_mainbz_all():
    ## 建立主营业务表
    print("开始建立主营业务表")
    for i, ts_code in enumerate(df_stock_lists.ts_code):
        time1 = time.time()
        df = get_fina_mainbz(ts_code)  ## todo concat

        if i % 100 == 0:
            print(f"success for : {i + 1}")

        sleep(max(0, 1 - (time.time() - time1)) + 0.05)

        # if i > 10:
        #     break
    print("Dowanload all fina mainbz done!")


def get_daily_basic_all(ts_codes=None, start_date='20180101'):
    """
    获取每日行情数据
    :param ts_codes: 列表或者set
    :param start_date:
    """
    print(f"Get data for daily basic from date: {start_date}")
    if not ts_codes:
        ts_codes = set(df_stock_lists.ts_code)
        print("Set ts_codes to all stock codes.")

    str_ts_code = []
    for i, ts_code in enumerate(ts_codes):
        if i % 100 == 0:
            print(f"Success num: {i + 1}")
        time1 = time.time()

        # cur.execute(f"select trade_date from daily_basic where ts_code='{ts_code}' limit 1")
        # row_1 = cur.fetchone()
        # try:
        #     df = pd.read_sql(f"select trade_date from daily_basic where ts_code='{ts_code}' limit 1", engine)
        #     if len(df)>0:
        #         continue
        # except:
        #     pass

        str_ts_code += [ts_code]
        if i % 11 != 10 and i < len(ts_codes) - 1:
            continue
        if len(ts_code) == 0: continue

        str_ts_code = ",".join(str_ts_code)
        df = pro.daily_basic(ts_code=str_ts_code, start_date=start_date, limit=10000)

        to_mysql(df, "daily_basic", "append")

        sleep(max(0, 1 - (time.time() - time1)) + 0.05)
        str_ts_code = []

    print(f"Get data from {start_date} success.")

def incre_daily_basic():
    '''
    从当前表中最大日期的下一个交易日开始，补充所有股票数据
    :return:
    '''
    df = pd.read_sql(f"select max(trade_date) from daily_basic limit 1", engine)
    trade_date = df.iloc[0,0]
    cur_date_ = datetime.datetime.strptime(trade_date,DATE_FORMAT)
    start_date = (cur_date_ + datetime.timedelta(days=1)).strftime(DATE_FORMAT)
    get_daily_basic_all(start_date=start_date)
    print("success.")


def get_daily_code_date(ts_code, start_date='20180101'):
    '''
    获取某一只股票从 start_date开始的交易数据
    :param ts_code:
    :param start_date:
    :return:
    '''
    try:
        pd.read_sql(f"""
                delete from daily_basic where ts_code='{ts_code}'
                and trade_date >= '{start_date}'
                """, engine)
    except:
        pass
    print(f"delete data for {ts_code} success.")
    df = pro.daily_basic(ts_code=ts_code, start_date=start_date, limit=10000)

    to_mysql(df, "daily_basic", "append")
    print(f"Get data for {ts_code} from f{start_date} success.")
    return df


def get_daily_basic_by_date(trade_date=None):
    '''
    获取某一个交易日的所有股票行情，若日期非交易日，则获取往前最近一个交易日
    :param trade_date:
    :return:
    '''
    if not trade_date:
        trade_date = datetime.datetime.today().strftime(DATE_FORMAT)
    while True:
        print(f"start get data for {trade_date}")
        try:
            df = pd.read_sql(f"select * from data_{trade_date}", engine)
            print(f'Get data from mysql success for : {trade_date}')
        except:
            df = pro.query('daily_basic', ts_code='', trade_date=trade_date,)
            if len(df)>0:
                to_mysql(df, f'data_{trade_date}', "replace")
                print(f'Get data from tushare success for : {trade_date}')
        if len(df) > 0:
            break
        else:
            trade_date = (datetime.datetime.strptime(trade_date,DATE_FORMAT) - datetime.timedelta(days=1)).strftime(DATE_FORMAT)

    return df


if __name__ == '__main__':
    # ## 初次建表 财务指标
    # engine.execute("drop table if exists fina_indicator ")
    # download_fina_indicator_all(start_date='20141201')

    # df = get_fina_indicator('000001.SZ')

    ## 全量主营业务
    # get_fina_mainbz('000002.SZ')
    # engine.execute("truncate table fina_indicator ")
    # download_fina_mainbz_all()

    ## 每日指标
    # get_daily_basic_all()

    # incre_daily_basic()

    ## 获取某一天的数据
    # get_daily_code_date("002027.SZ")
    get_daily_basic_by_date(trade_date='2020430')

    print('done')
