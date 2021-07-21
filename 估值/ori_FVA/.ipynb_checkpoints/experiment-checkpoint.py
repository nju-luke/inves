# -*- coding:utf-8 -*-
"""
author: Luke
datettime: 2020/4/5 1:13
"""
from ori_FVA.utils import Company, CompanyPredict, dump_company
import pandas as pd

code = '600519'
date = '20181231'


growth_rate = 1.1
invest_increase_rate = 1.15

# df_profit_statement, df_balance_sheet, df_cash_flow = get_data(code, date)

# df_profit_statement.T.to_csv('data/prof.csv')
# df_balance_sheet.T.to_csv('data/bala.csv')

df_profit_statement = pd.read_csv('data/prof.csv', encoding='gb2312', index_col=0).T.fillna(0)
df_balance_sheet = pd.read_csv('data/bala.csv', encoding='gb2312', index_col=0).T.fillna(0)

df_profit_statement['date'] = df_profit_statement['date'].astype(int).astype(str)
df_balance_sheet['date'] = df_balance_sheet['date'].astype(int).astype(str)

df_cash_flow = df_balance_sheet
base_company = Company(df_profit_statement, df_balance_sheet, df_cash_flow, date)


company_flow = [base_company]


# todo 目前看起来财务费用的问题不大，问题出在FCF等的计算上？

# todo 增长率
growth_rates = [growth_rate] * 5
growth_rate_perp = 1.05
for g in growth_rates:
    cmp_ = CompanyPredict(company_flow[-1], g, invest_increase_rate)
    company_flow.append(cmp_)

dump_company(code, company_flow)

print('done')

# sjf_2019_e.profit_statement.T.to_csv('profit_statement.csv')
# sjf_2019_e.balance_sheet.T.to_csv('balance_sheet.csv')
#
# sjf_20181231.profit_statement.T.to_csv('profit_statement1.csv')
# sjf_20181231.balance_sheet.T.to_csv('balance_sheet1.csv')
#

## 两种估值方法的应用

