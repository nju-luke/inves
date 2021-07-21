# -*- coding:utf-8 -*-
"""
author: Luke
datettime: 2021/7/21 0:13
"""
from urllib.request import urlretrieve

import pandas as pd
import os
import urllib
base_dir = 'data/'


from sqlalchemy import create_engine, VARCHAR

engine = create_engine('mysql+pymysql://root:00000@localhost:3306/stocks_163')

eporttype = {'利润表': {'url': 'http://quotes.money.163.com/service/lrb_{}.html', 'table':"lrb"},
              '主要财务指标': {'url': 'http://quotes.money.163.com/service/zycwzb_{}.html', 'table':"zycwzb"},
              '资产负债表': {'url': 'http://quotes.money.163.com/service/zcfzb_{}.html', 'table':"zcfzb"},
              '财务报表摘要': {'url': 'http://quotes.money.163.com/service/cwbbzy_{}.html', 'table':"cwbbzy"},
              '盈利能力': {'url': 'http://quotes.money.163.com/service/zycwzb_{}.html?part=ylnl', 'table':"zycwzb"},
              '偿还能力': {'url': 'http://quotes.money.163.com/service/zycwzb_{}.html?part=chnl', 'table':"zycwzb"},
              '成长能力': {'url': 'http://quotes.money.163.com/service/zycwzb_{}.html?part=cznl', 'table':"zycwzb"},
              '营运能力': {'url': 'http://quotes.money.163.com/service/zycwzb_{}.html?part=yynl', 'table':"zycwzb"},
              '现金流量表': {'url': 'http://quotes.money.163.com/service/xjllb_{}.html', 'table':"xjllb"}}

codes = [600779]



def save_to_sql(code, key, file_path, table_name):
    df = pd.read_csv(file_path, encoding="gb2312").replace("--",-1)
    df.replace("--",-1)
    try:
        engine.execute(f"DELETE FROM {table_name} WHERE code={code}")
    except:
        pass

    df.dropna(inplace=True, axis=1)
    df['报告日期'] = df['报告日期'].apply(lambda x: x.replace("(万元)", ""))

    df_names = pd.read_excel("网易财经数据字典.xlsx", key)

    df = pd.merge(df_names,df, left_on='zh_name', right_on='报告日期')
    df1 = df.set_index("en_name", drop=True).drop(["报告日期","zh_name"], axis=1).T

    for col in df1.columns:
        try:
            df1[col] = df1[col].astype(int)
        except:
            df1[col] = df1[col].astype(float)


    df1.reset_index(inplace=True)
    df1.rename(columns={'index': 'date'}, inplace=True)
    df1.insert(0, "code", code)

    df1.to_sql(table_name, engine, if_exists="append", index=False, dtype={"date":VARCHAR(10)})

    print(f" save {key} of {code} to {table_name} success.")



def get_data(code):
    print(f'Start downloading files for {code}')
    for key, value in eporttype.items():
        url = value['url']
        table_name = value['table']
        file_path = base_dir + f'{code}_{key}.csv'
        url = url.format(code)
        # urllib.request.urlretrieve(url, file_path)
        urlretrieve(url, file_path)

        save_to_sql(code, key, file_path, table_name)

        print(f'Successfully download {key}')
    print(f'Done for {code}')

get_data(codes[0])