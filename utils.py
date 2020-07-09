# -*- coding:utf-8 -*-
"""
author: byangg
datettime: 7/9/2020 10:21
"""

from sqlalchemy import create_engine, VARCHAR

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
