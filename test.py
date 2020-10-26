# -*- coding:utf-8 -*-
"""
author: Luke
datettime: 2020/7/6 21:01
"""

code_dict = {'603589': '口子窖',
             '000596': '古井贡酒',
             '603189': '迎驾贡酒',
             '600199': '金种子酒'}

for k, v in list(code_dict.items()):
    if "." in k: continue
    surfix = '.SH' if k.startswith('6') else '.SZ'
    code_dict.pop(k)
    code_dict[k + surfix] = v

data_df['oper_profit'] = data_df['revenue'] - (
        data_df[["oper_cost", "int_exp", "comm_exp", "biz_tax_surchg", "sell_exp", "admin_exp", "prem_refund",
                 "compens_payout", "reser_insur_liab", "div_payt", "reins_exp", "compens_payout_refu",
                 "insur_reser_refu", "reins_cost_refund", "other_bus_cost"]] + data_df['fin_exp'].apply(
    lambda x: x if x > 0 else 0) + data_df['assets_impair_loss'].apply(lambda x: x if x > 0 else 0)
)
