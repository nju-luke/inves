# -*- coding:utf-8 -*-
"""
author: byangg
datettime: 7/9/2020 09:58
"""

'''
获取实时数据，放在data.xlsx中，可供统计excel直接调用
'''

import datetime

import pandas as pd

from get_data_tspro import get_daily_code_date, df_stock_lists

s_list = ["分众传媒",
          "洋河股份",
          "海康威视",
          "金溢科技",
          "中国平安",
          "口子窖", ]
cur_date = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime("%Y%m%d")

df_base = df_stock_lists[df_stock_lists.name.isin(s_list)].loc[:, ["ts_code", "name"]]
str_codes = ",".join(df_base.ts_code)

df_all = get_daily_code_date(str_codes, cur_date)
df_all = pd.merge(df_base, df_all, on='ts_code')

excel_path = "D:/synchronize/投资理财/data.xlsx"
df_all.to_excel(excel_path)


print('done')
