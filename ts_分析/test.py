# -*- coding:utf-8 -*-
"""
author: Luke
datettime: 2020/7/6 21:01
"""

with fig.batch_update():
    fig.data[2].update(yaxis='y5')
    fig.layout.update(yaxis5=dict(overlaying='y', side='right', anchor='x', showgrid=False), hovermode='closest')
    fig.data[5].update(yaxis='y6')
    fig.layout.update(yaxis5=dict(overlaying='y2', side='right', anchor='x2', showgrid=False), hovermode='closest')
    fig.data[8].update(yaxis='y7')
    fig.layout.update(yaxis5=dict(overlaying='y3', side='right', anchor='x3', showgrid=False), hovermode='closest')
    fig.data[11].update(yaxis='y8')
    fig.layout.update(yaxis5=dict(overlaying='y4', side='right', anchor='x4', showgrid=False), hovermode='closest')
