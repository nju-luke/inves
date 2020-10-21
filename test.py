# -*- coding:utf-8 -*-
"""
author: Luke
datettime: 2020/7/6 21:01
"""

code_dict = {'603589':'口子窖',
            '000596':'古井贡酒',
            '603189':'迎驾贡酒',
            '600199':'金种子酒'}

for k,v in list(code_dict.items()):
    if "." in k: continue
    surfix = '.SH' if k.startswith('6') else '.SZ'
    code_dict.pop(k)
    code_dict[k+surfix] = v



