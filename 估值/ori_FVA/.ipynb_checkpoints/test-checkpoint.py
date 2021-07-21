# -*- coding:utf-8 -*-
"""
author: Luke
datettime: 2020/4/3 19:51
"""

# import pandas as pd
# v = pd.__version__
# if int(v.split('.')[1])>=25 or int(v.split('.')[0])>0:
#     from io import StringIO
# else:
#     from pandas.compat import StringIO
# from tushare.util import dateu as du
# try:
#     from urllib.request import urlopen, Request
# except ImportError:
#     from urllib2 import urlopen, Request
#
# urlt = "http://quotes.money.163.com/service/zcfzb_601318.html"
#
# def get_zcfzb():
#     """
#         获取某股票的历史所有时期利润表
#     Parameters
#     --------
#     code:str 股票代码 e.g:600518
#
#     Return
#     --------
#     DataFrame
#         行列名称为中文且数目较多，建议获取数据后保存到本地查看
#     """
#     # if code.isdigit():
#     request = Request(urlt)#ct.SINA_PROFITSTATEMENT_URL%(code))
#     text = urlopen(request, timeout=10).read()
#     text = text.decode('GBK')
#     text = text.replace('\t\n', '\r\n')
#     text = text.replace('\t', ',')
#     df = pd.read_csv(StringIO(text), dtype={'code':'object'})
#     return df
#
# df = get_zcfzb()
# print(df.shape)


items = '''
短期借款
一年内到期的非流动负债
所有者权益(或股东权益)合计
长期借款
应付债券
租赁负债
长期应付职工薪酬
长期应付款
专项应付款
预计非流动负债
递延所得税负债
长期递延收益
其他非流动负债
'''.strip().split()
print(items)
