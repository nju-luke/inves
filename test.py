import tushare as ts
df = ts.get_profit_statement('603195')
df.index = df['报表日期']
df.drop('报表日期',axis=1,inplace=True)
df.drop('单位',inplace=True)
df = df.astype(float)/1e6
print(df)