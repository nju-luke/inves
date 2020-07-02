# -*- coding:utf-8 -*-
"""
author: Luke
datettime: 2020/6/12 0:54
"""
import tushare as ts

df = ts.get_today_all()


from sqlalchemy import create_engine,VARCHAR, CHAR
engine = create_engine('mysql+pymysql://root:00000@localhost:3306/stocks')

print('done')