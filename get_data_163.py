# -*- coding:utf-8 -*-
"""
author: Luke
datettime: 2021/7/21 0:13
"""
from io import StringIO
from urllib.request import urlretrieve

import pandas as pd
import os
import urllib

import requests

base_dir = 'data/'

from sqlalchemy import create_engine, VARCHAR

engine = create_engine('mysql+pymysql://root:00000@localhost:3306/stocks_163')

eporttype = {
    '利润表': {'url': 'http://quotes.money.163.com/service/lrb_{}.html', 'table': "lrb"},
    '资产负债表': {'url': 'http://quotes.money.163.com/service/zcfzb_{}.html', 'table': "zcfzb"},
    '现金流量表': {'url': 'http://quotes.money.163.com/service/xjllb_{}.html', 'table': "xjllb"},
    '财务报表摘要': {'url': 'http://quotes.money.163.com/service/cwbbzy_{}.html', 'table': "cwbbzy"},
    '主要财务指标': {'url': 'http://quotes.money.163.com/service/zycwzb_{}.html', 'table': "zycwzb"},
    '盈利能力': {'url': 'http://quotes.money.163.com/service/zycwzb_{}.html?part=ylnl', 'table': "zycwzb_ylnl"},
    '偿还能力': {'url': 'http://quotes.money.163.com/service/zycwzb_{}.html?part=chnl', 'table': "zycwzb_chnl"},
    '成长能力': {'url': 'http://quotes.money.163.com/service/zycwzb_{}.html?part=cznl', 'table': "zycwzb_cznl"},
    '营运能力': {'url': 'http://quotes.money.163.com/service/zycwzb_{}.html?part=yynl', 'table': "zycwzb_yynl"},
}
keys = [
'利润表',
 '资产负债表',
 '现金流量表',
 '财务报表摘要',
 '主要财务指标',
 '盈利能力',
 '偿还能力',
 '成长能力',
 '营运能力'
 ]
tables = [
'lrb',
 'zcfzb',
 'xjllb',
 'cwbbzy',
 'zycwzb',
 'zycwzb_ylnl',
 'zycwzb_chnl',
 'zycwzb_cznl',
 'zycwzb_yynl'
 ]



def save_to_sql(code, key, df, table_name):
    # df = pd.read_csv(file_path, encoding="gb2312").replace("--",-1)
    # df = df.replace(" ", "").replace("--", -1)
    df = df.rename(columns={"报告期": "报告日期"})
    try:
        engine.execute(f"DELETE FROM {table_name} WHERE code={code}")
    except:
        pass

    df.dropna(inplace=True, axis=1)

    # df['报告日期'] = df.iloc[:, 0].apply(lambda x: x.replace("(万元)", ""))
    df['报告日期'] = df['报告日期'].apply(lambda x: x.replace("(万元)", ""))

    df_names = pd.read_excel("网易财经数据字典.xlsx", key)

    df = pd.merge(df_names, df, left_on='zh_name', right_on='报告日期')
    df1 = df.set_index("en_name", drop=True).drop(["报告日期", "zh_name", "ori_name"], axis=1).T

    for col in df1.columns:
        try:
            df1[col] = df1[col].astype(int)
        except:
            df1[col] = df1[col].astype(float)

    df1.reset_index(inplace=True)
    df1.rename(columns={'index': 'date'}, inplace=True)
    df1.insert(0, "code", code)

    df1.to_sql(table_name, engine, if_exists="append", index=False, dtype={"date": VARCHAR(10)})

    print(f" save {key} of {code} to {table_name} success.")
    return df1


headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:66.0) Gecko/20100101 Firefox/66.0"}


def download_data(url):
    req = requests.get(url, headers=headers)
    text = req.text.replace("--", "-1").replace(" ","")
    data = StringIO(text)

    df = pd.read_csv(data)
    return df


def get_data_163(code):
    print(f'Start downloading files for {code}')
    res = []
    for key in keys:
        value = eporttype[key]
        url = value['url']
        table_name = value['table']
        file_path = base_dir + f'{code}_{key}.csv'
        url = url.format(code)

        # urllib.request.urlretrieve(url, file_path)
        # urlretrieve(url, file_path)

        df = download_data(url)
        df = save_to_sql(code, key, df, table_name)

        print(f'Successfully process {key}')
        res.append(df)
    print(f'Done for {code}')
    return res

def get_data(code, end_date):
    res = []
    for key in keys:
        value = eporttype[key]
        url = value['url'].format(code)
        table_name = value['table']

        df = pd.read_sql(f"select * from {table_name} where code={code}", engine)

        if len(df) == 0 or (end_date is not None and max(df['date']) < end_date):
            df = download_data(url)
            df = save_to_sql(code, key, df, table_name)
        else:
            print(f"get {key} from mysql for {code} success.")

        res.append(df)
    return res


# def get_data_mysql(code, end_date):
#     res = []
#     for table_name in tables:
#         df = pd.read_sql(f"select * from {table_name} where code={code}", engine)
#
#         if len(df) == 0 or (end_date is not None and max(df['date']) < end_date):  # todo 日期
#             raise ValueError
#
#         res.append(df)
#     return res


class FinData:
    def __init__(self, code, end_date=None, date_filter=None):
        self.code = code
        self.date_filter = date_filter
        self.get_data(end_date)

    def get_data(self, end_date):
        res = get_data(self.code, end_date)
        for i, df in enumerate(res):
            res[i] = df[df['date'].apply(lambda x: x.endswith(self.date_filter))]

        self.lrb, self.zcfzb, self.xjllb, self.cwbbzy, \
        self.zycwzb, self.zycwzb_ylnl, self.zycwzb_chnl, \
        self.zycwzb_cznl, self.zycwzb_yynl = res



if __name__ == '__main__':
    # code = 600779  # 水井坊
    # code = 601225   # 陕西煤业
    code = 600519

    fd = FinData(code, date_filter="12-31")


    print('done')