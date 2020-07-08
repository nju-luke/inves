''''''
"""
fina_indicator
名称 as "描述",
ts_code as "TS代码",
ann_date as "公告日期",
end_date as "报告期",
eps as "基本每股收益",
dt_eps as "稀释每股收益",
total_revenue_ps as "每股营业总收入",
revenue_ps as "每股营业收入",
capital_rese_ps as "每股资本公积",
surplus_rese_ps as "每股盈余公积",
undist_profit_ps as "每股未分配利润",



-- 风险
current_ratio as "流动比率",
quick_ratio as "速动比率",
cash_ratio as "保守速动比率",

-- 运营
ar_turn as "应收账款周转率",
ca_turn as "流动资产周转率",
fa_turn as "固定资产周转率",
assets_turn as "总资产周转率",


-- 利润
extra_item as "非经常性损益",
profit_dedt as "扣除非经常性损益后的净利润",

op_income as "经营活动净收益",
ebit as "息税前利润",
ebitda as "息税折旧摊销前利润",

npta as "总资产净利润",

-- 现金流
fcff as "企业自由现金流量",
fcfe as "股权自由现金流量",

-- 负债
current_exint as "无息流动负债",
noncurrent_exint as "无息非流动负债",
interestdebt as "带息债务",
netdebt as "净债务",


tangible_asset as "有形资产",
working_capital as "营运资金",
networking_capital as "营运流动资本",
invest_capital as "全部投入资本",
retained_earnings as "留存收益",
diluted2_eps as "期末摊薄每股收益",
bps as "每股净资产",
ocfps as "每股经营活动产生的现金流量净额",
retainedps as "每股留存收益",
cfps as "每股现金流量净额",
ebit_ps as "每股息税前利润",
fcff_ps as "每股企业自由现金流量",
fcfe_ps as "每股股东自由现金流量",

-- 毛利成本
gross_margin as "毛利",
netprofit_margin as "销售净利率",
grossprofit_margin as "销售毛利率",
cogs_of_sales as "销售成本率",
expense_of_sales as "销售期间费用率",

-- 结构分析
profit_to_gr as "净利润/营业总收入",
saleexp_to_gr as "销售费用/营业总收入",
adminexp_of_gr as "管理费用/营业总收入",
finaexp_of_gr as "财务费用/营业总收入",
gc_of_gr as "营业总成本/营业总收入",
op_of_gr as "营业利润/营业总收入",
ebit_of_gr as "息税前利润/营业总收入",

impai_ttm as "资产减值损失/营业总收入",

-- ROE ROA
roe as "净资产收益率",
roe_waa as "加权平均净资产收益率",
roe_dt as "净资产收益率(扣除非经常损益)",
roa as "总资产报酬率",
roic as "投入资本回报率",
roe_yearly as "年化净资产收益率",
roa2_yearly as "年化总资产报酬率",
roa_yearly as "年化总资产净利率",
roa_dp as "总资产净利率(杜邦分析)",

-- 杠杆
debt_to_assets as "资产负债率",
assets_to_eqt as "权益乘数",
dp_assets_to_eqt as "权益乘数(杜邦分析)",
currentdebt_to_debt as "流动负债/负债合计",
longdeb_to_debt as "非流动负债/负债合计",


-- 行业类型
ca_to_assets as "流动资产/总资产",
nca_to_assets as "非流动资产/总资产",
tbassets_to_totalassets as "有形资产/总资产",
int_to_talcap as "带息债务/全部投入资本",
eqt_to_talcapital as "归属于母公司的股东权益/全部投入资本",


ocf_to_shortdebt as "经营活动产生的现金流量净额/流动负债",
debt_to_eqt as "产权比率",
eqt_to_debt as "归属于母公司的股东权益/负债合计",
eqt_to_interestdebt as "归属于母公司的股东权益/带息债务",
tangibleasset_to_debt as "有形资产/负债合计",
tangasset_to_intdebt as "有形资产/带息债务",
tangibleasset_to_netdebt as "有形资产/净债务",
ocf_to_debt as "经营活动产生的现金流量净额/负债合计",
turn_days as "营业周期",
fixed_assets as "固定资产合计",
profit_to_op as "利润总额／营业收入",

-- 单季度
q_saleexp_to_gr as "销售费用／营业总收入 as "(单季度)",
q_gc_to_gr as "营业总成本／营业总收入 as "(单季度)",
q_roe as "净资产收益率(单季度)",
q_dt_roe as "净资产单季度收益率(扣除非经常损益)",
q_npta as "总资产净利润(单季度)",
q_ocf_to_sales as "经营活动产生的现金流量净额／营业收入(单季度)",
q_sales_yoy as "营业收入同比增长率(%)(单季度)",
q_op_qoq as "营业利润环比增长率(%)(单季度)",

-- 增长
tr_yoy as "营业总收入同比增长率(%)",
or_yoy as "营业收入同比增长率(%)",
op_yoy as "营业利润同比增长率(%)",
ebt_yoy as "利润总额同比增长率(%)",
netprofit_yoy as "归属母公司股东的净利润同比增长率(%)",
ocf_yoy as "经营活动产生的现金流量净额同比增长率(%)",
dt_netprofit_yoy as "归属母公司股东的净利润-扣除非经常损益同比增长率(%)",
roe_yoy as "净资产收益率(摊薄)同比增长率(%)",
basic_eps_yoy as "基本每股收益同比增长率(%)",
dt_eps_yoy as "稀释每股收益同比增长率(%)",
cfps_yoy as "每股经营活动产生的现金流量净额同比增长率(%)",
bps_yoy as "每股净资产相对年初增长率(%)",
assets_yoy as "资产总计相对年初增长率(%)",
eqt_yoy as "归属母公司的股东权益相对年初增长率(%)",
equity_yoy as "净资产同比增长率",




名称 as "描述",
ts_code as "TS股票代码",
trade_date as "交易日期",
close as "当日收盘价",
turnover_rate as "换手率（%）",
turnover_rate_f as "换手率（自由流通股）",
volume_ratio as "量比",
pe as "市盈率（总市值/净利润，亏损的PE为空）",
pe_ttm as "市盈率（TTM，亏损的PE为空）",
pb as "市净率（总市值/净资产）",
ps as "市销率",
ps_ttm as "市销率（TTM）",
dv_ratio as "股息率（%）",
dv_ttm as "股息率（TTM）（%）",
total_share as "总股本（万股）",
float_share as "流通股本（万股）",
free_share as "自由流通股本（万）",
total_mv as "总市值（万元）",
circ_mv as "流通市值（万元）",


"""