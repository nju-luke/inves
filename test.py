# -*- coding:utf-8 -*-
"""
author: Luke
datettime: 2020/7/6 21:01
"""
from pymysql import ProgrammingError
from sqlalchemy import create_engine
import pymysql

import time

engine = create_engine('mysql+pymysql://root:00000@localhost:3306/stocks')
conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='00000', db='stocks')
cur = conn.cursor()

def test1():
    for i in range(1000):
        cur.execute("select trade_date from daily_basic where ts_code='000001.SZ' limit 1")

        row_1 = cur.fetchone()
        if row_1 != None:
            continue

import pandas as pd
def test2():
    for i in range(1000):
        try:
            df_exists = pd.read_sql(f"select trade_date from daily_basic where ts_code='000001.SZ' limit 1", engine)
            if len(df_exists) >0:
                continue
        except ProgrammingError:
            pass


time1 = time.time()
test1()
print(time.time()-time1)

print(1)


