# -*- coding:utf-8 -*-
"""
author: Luke
datettime: 2020/4/3 0:33
"""
import tushare as ts
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, VARCHAR, CHAR

engine = create_engine('mysql+pymysql://root:00000@localhost:3306/stocks')

UNIT = 1000000  # 百万
DELTA = 0.5  # 配平资产负债表的容忍值
effective_interest_rate = 0.04
EPS = 1e-8

cols_operation_cost = ['营业成本', '营业税金及附加', '销售费用', '管理费用', '研发费用']
cols_operation_assets = ['货币资金', '交易性金融资产', '衍生金融资产', '应收票据', '应收账款', '应收款项融资', '预付款项',
                         '应收利息', '应收股利', '其他应收款', '买入返售金融资产', '存货', '划分为持有待售的资产',
                         '一年内到期的非流动资产', '待摊费用', '待处理流动资产损益', '其他流动资产', '在建工程',
                         '工程物资', '固定资产净额', '固定资产清理', '生产性生物资产', '公益性生物资产', '油气资产',
                         '使用权资产', '无形资产', '开发支出', '长期待摊费用']
cols_operation_debt = ['交易性金融负债', '应付票据', '应付账款', '预收款项', '应付手续费及佣金', '应付职工薪酬', '应交税费',
                       '应付利息', '应付股利', '其他应付款', '预提费用', '一年内的递延收益', '应付短期债券',
                       '一年内到期的非流动负债', '其他流动负债']
cols_equity = ['实收资本(或股本)', '资本公积', '减：库存股', '其他综合收益', '专项储备', '盈余公积', '一般风险准备', '未分配利润']
cols_debt = ['短期借款', '交易性金融负债', '应付票据', '应付账款', '预收款项', '应付手续费及佣金', '应付职工薪酬',
             '应交税费', '应付利息', '应付股利', '其他应付款', '预提费用', '一年内的递延收益', '应付短期债券',
             '一年内到期的非流动负债', '其他流动负债', '长期借款', '应付债券', '租赁负债', '长期应付职工薪酬',
             '长期应付款', '专项应付款', '预计非流动负债', '递延所得税负债', '长期递延收益', '其他非流动负债']
cols_debt_long_ex_loan = ['应付债券', '租赁负债', '长期应付职工薪酬', '长期应付款', '专项应付款', '预计非流动负债', '递延所得税负债',
                          '长期递延收益', '其他非流动负债']
cols_assets_long = ['发放贷款及垫款', '可供出售金融资产', '持有至到期投资', '长期应收款', '长期股权投资', '投资性房地产',
                    '商誉', '递延所得税资产', '其他非流动资产']
cols_assets = ['货币资金', '交易性金融资产', '衍生金融资产', '应收票据', '应收账款', '应收款项融资', '预付款项', '应收利息',
               '应收股利', '其他应收款', '买入返售金融资产', '存货', '划分为持有待售的资产', '一年内到期的非流动资产',
               '待摊费用', '待处理流动资产损益', '其他流动资产', '发放贷款及垫款', '可供出售金融资产', '持有至到期投资',
               '长期应收款', '长期股权投资', '投资性房地产', '在建工程', '工程物资', '固定资产净额', '固定资产清理',
               '生产性生物资产', '公益性生物资产', '油气资产', '使用权资产', '无形资产', '开发支出', '商誉', '长期待摊费用',
               '递延所得税资产', '其他非流动资产']
cols_flow_operation_assets = ['货币资金', '交易性金融资产', '衍生金融资产', '应收票据', '应收账款', '应收款项融资',
                              '预付款项', '应收利息', '应收股利', '其他应收款', '买入返售金融资产']
cols_flow_operation_debt = ['短期借款', '交易性金融负债', '应付票据', '应付账款', '预收款项', '应付手续费及佣金', '应付职工薪酬', '应交税费']


class CompanyBase():
    def __init__(self):
        self.balance_sheet = pd.DataFrame()
        self.profit_statement = pd.DataFrame()
        self.cash_flow = pd.DataFrame()
        self.growth_rate = 1

    @property  # 经营性资产
    def operating_assets(self):
        return np.sum(self.balance_sheet.loc[0, cols_operation_assets])

    @property  # 经营性负债
    def operating_debts(self):
        return np.sum(self.balance_sheet.loc[0, cols_operation_debt])

    @property
    def OPNA(self):
        return np.sum(self.balance_sheet.loc[0, ['短期借款', '一年内到期的非流动负债', '所有者权益(或股东权益)合计',
                                                 '长期借款', '应付债券', '租赁负债', '长期应付职工薪酬', '长期应付款',
                                                 '专项应付款', '预计非流动负债', '递延所得税负债', '长期递延收益',
                                                 '其他非流动负债']
                      ])


class Company(CompanyBase):
    def __init__(self, profit_statement, balance_sheet, cash_flow,
                 report_date=None):
        self.report_date = report_date
        self.build_profit_statement(profit_statement)
        self.build_profit_balance_sheet(balance_sheet)
        self.build_profit_cash_flow(cash_flow)

    def build_profit_statement(self, profit_statement):
        df = profit_statement[profit_statement.date == self.report_date]
        df.index = [0]
        # 细分每一个项目
        self.profit_statement = df  # .to_dict('records')[0]

    def build_profit_balance_sheet(self, balance_sheet):
        df = balance_sheet[balance_sheet.date == self.report_date]
        df.index = [0]
        self.balance_sheet = df  # .to_dict('records')[0]

    def build_profit_cash_flow(self, cash_flow):
        df = cash_flow[cash_flow.date == self.report_date]
        df.index = [0]
        self.cash_flow = df  # .to_dict('records')[0]

    @property  # 短期、长期借款，股权比率
    def debt_ratio(self):
        dq = self.balance_sheet.loc[0, '短期借款']
        cq = self.balance_sheet.loc[0, '长期借款']
        qy = self.balance_sheet.loc[0, '所有者权益(或股东权益)合计']
        fz = self.balance_sheet.loc[0, '负债合计']
        zc = self.balance_sheet.loc[0, '资产总计']
        return fz / zc * dq / (dq + cq + EPS), \
               fz / zc * cq / (dq + cq + EPS), \
               qy / zc

    @property
    def tax_rate(self):
        return self.profit_statement.loc[0, '减：所得税费用'] / self.profit_statement.loc[0, '四、利润总额']

    @property
    def effective_interest_rate(self):
        return self.profit_statement.loc[0, '财务费用'] / (self.balance_sheet.loc[0, '短期借款'] +
                                                       self.balance_sheet.loc[0, '长期借款'])


# todo 衰退型行业
class CompanyPredict(CompanyBase):
    def __init__(self, company, growth_rate, invest_increase_rate=0.):
        """

        :param company:
        :param growth_rate: 增长率为实际增长率，如: 1.1
        :param invest_increase_rate: 投资增长率,如： 1.15
        """
        self.base_company = company
        self.growth_rate = growth_rate
        if growth_rate == 1:  # todo 测试
            self = company
            return

        self.debt_ratio = company.debt_ratio
        self.tax_rate = company.tax_rate
        self.effective_interest_rate = company.effective_interest_rate
        self.invest_increase_rate = invest_increase_rate

        self.pred_operation()  # 预测经营活动
        self.pred_operating_assets()  # 预测经营性资产
        self.pred_operating_debt()  # 预测经营性负债
        self.pred_financing()  # 预测融资活动
        self.pred_others()
        self.adjust()  # 调平报表
        self.build_cash_flow()  # 生成现金流量表

    def pred_operation(self):
        self.profit_statement = self.base_company.profit_statement * 0
        self.profit_statement.loc[0, '营业收入'] = self.base_company.profit_statement.loc[0, '营业收入'] * self.growth_rate
        self.profit_statement.loc[0, '投资收益'] = self.base_company.profit_statement.loc[
                                                   0, '投资收益'] * self.invest_increase_rate
        self.profit_statement.loc[0, '其中:对联营企业和合营企业的投资收益'] = \
            self.base_company.profit_statement.loc[0, '其中:对联营企业和合营企业的投资收益']
        self.profit_statement.loc[0, '加:营业外收入'] = self.base_company.profit_statement.loc[0, '加:营业外收入']
        self.profit_statement.loc[0, '减：营业外支出'] = self.base_company.profit_statement.loc[0, '减：营业外支出']
        self.profit_statement.loc[0, '其中：非流动资产处置损失'] = \
            self.base_company.profit_statement.loc[0, '其中：非流动资产处置损失']

        self.profit_statement.loc[0, cols_operation_cost] = \
            self.base_company.profit_statement.loc[0, cols_operation_cost] * self.growth_rate

    def pred_operating_assets(self):
        self.balance_sheet = self.base_company.balance_sheet * 0
        self.balance_sheet.loc[0, cols_operation_assets] = \
            self.base_company.balance_sheet.loc[0, cols_operation_assets] * self.growth_rate

        # self.balance_sheet.loc[0, '短期借款'] = self.base_company.balance_sheet.loc[0, '短期借款']
        self.balance_sheet.loc[0, '长期借款'] = self.base_company.balance_sheet.loc[0, '长期借款']

    def pred_operating_debt(self):
        self.balance_sheet.loc[0, cols_operation_debt] = \
            self.base_company.balance_sheet.loc[0, cols_operation_debt] * self.growth_rate

    # def finacing_need(self):
    #     return (self.operating_assets - self.operating_debts) / self.growth_rate * (self.growth_rate - 1)

    # ## todo 营业利润之间的关系，可能不需要计算这个
    # @property  # 营业利润
    # def operating_profit(self):
    #     self.profit_statement.loc[0, '三、营业利润'] = self.profit_statement.loc[0, '营业收入'] \
    #                                              - np.sum(self.profit_statement.loc[0, cols_operation_cost]) \
    #                                              + self.profit_statement.loc[0, '投资收益'] \
    #                                              + self.profit_statement.loc[0, '汇兑收益']
    #
    #     return self.profit_statement.loc[0, '三、营业利润']

    @property  # 净利润
    def net_profit(self):
        self.profit_statement.loc[0, '三、营业利润'] = self.profit_statement.loc[0, '营业收入'] \
                                                 - np.sum(self.profit_statement.loc[0, cols_operation_cost]) \
                                                 - self.profit_statement.loc[0, '财务费用'] \
                                                 - self.profit_statement.loc[0, '资产减值损失'] \
                                                 + self.profit_statement.loc[0, '公允价值变动收益'] \
                                                 + self.profit_statement.loc[0, '投资收益'] \
                                                 + self.profit_statement.loc[0, '汇兑收益']
        self.profit_statement.loc[0, '四、利润总额'] = self.profit_statement.loc[0, '三、营业利润'] \
                                                 + self.profit_statement.loc[0, '加:营业外收入'] \
                                                 - self.profit_statement.loc[0, '减：营业外支出']
        self.profit_statement.loc[0, '减：所得税费用'] = self.profit_statement.loc[0, '四、利润总额'] * self.tax_rate
        self.profit_statement.loc[0, '五、净利润'] = self.profit_statement.loc[0, '四、利润总额'] * (1 - self.tax_rate)
        return self.profit_statement.loc[0, '五、净利润']

    # self.profit_statement.loc[0, '四、利润总额'] = self.operating_profit
    # self.profit_statement.loc[0, '减：所得税费用'] = self.profit_statement.loc[0, '四、利润总额'] * self.tax_rate
    # self.profit_statement.loc[0, '五、净利润'] = self.profit_statement.loc[0, '四、利润总额'] - \
    #                                         self.profit_statement.loc[0, '减：所得税费用']
    # return self.profit_statement.loc[0, '五、净利润']

    @property
    def equity_all(self):
        self.balance_sheet.loc[0, '盈余公积'] = self.base_company.balance_sheet.loc[0, '盈余公积'] + self.net_profit * 0.1
        self.balance_sheet.loc[0, '未分配利润'] = self.base_company.balance_sheet.loc[0, '未分配利润'] + self.net_profit * 0.9
        self.balance_sheet.loc[0, '所有者权益(或股东权益)合计'] = np.sum(self.balance_sheet.loc[0, cols_equity])
        return self.balance_sheet.loc[0, '所有者权益(或股东权益)合计']

    @property
    def debt_all(self):
        self.balance_sheet.loc[0, '负债合计'] = np.sum(self.balance_sheet.loc[0, cols_debt])
        return self.balance_sheet.loc[0, '负债合计']

    @property
    def assets_all(self):
        self.balance_sheet.loc[0, '资产总计'] = np.sum(self.balance_sheet.loc[0, cols_assets])
        return self.balance_sheet.loc[0, '资产总计']

    @property
    def finance_fee(self):
        self.profit_statement.loc[0, '财务费用'] = self.effective_interest_rate * (self.balance_sheet.loc[0, '短期借款']
                                                                               + self.balance_sheet.loc[0, '长期借款'])
        return self.profit_statement.loc[0, '财务费用']

    def pred_financing(self):
        # 计算融资需要，分别分配到短期、长期、股本当中
        finacing_need = (self.operating_assets - self.operating_debts) / self.growth_rate * (self.growth_rate - 1)
        financing_dq = self.debt_ratio[0] * finacing_need
        financing_cq = self.debt_ratio[1] * finacing_need
        financing_gq = self.debt_ratio[2] * finacing_need

        # 新增财务费用
        # cost_finance_add = self.effective_interest_rate * (financing_dq + financing_cq)
        self.balance_sheet.loc[0, '短期借款'] += financing_dq
        self.balance_sheet.loc[0, '长期借款'] += financing_cq
        # self.profit_statement.loc[0, '财务费用'] =  cost_finance_add #+ self.base_company.profit_statement.loc[0, '财务费用']
        # print(f'net_profit: {self.net_profit}' )

        self.balance_sheet.loc[0, '实收资本(或股本)'] = self.base_company.balance_sheet.loc[0, '实收资本(或股本)'] + financing_gq

    def pred_others(self):
        # equity
        self.balance_sheet.loc[0, '资本公积'] = self.base_company.balance_sheet.loc[0, '资本公积']
        self.balance_sheet.loc[0, '归属于母公司股东权益合计'] = self.base_company.balance_sheet.loc[0, '归属于母公司股东权益合计']
        self.balance_sheet.loc[0, '少数股东权益'] = self.base_company.balance_sheet.loc[0, '少数股东权益']
        # print(f'equity_all: {self.equity_all}' )

        # debt
        self.balance_sheet.loc[0, cols_debt_long_ex_loan] = \
            self.base_company.balance_sheet.loc[0, cols_debt_long_ex_loan]
        # print(f'debt_all: {self.debt_all}' )

        # assets
        self.balance_sheet.loc[0, cols_assets_long] = self.base_company.balance_sheet.loc[0, cols_assets_long]
        self.balance_sheet.loc[0, '长期股权投资'] *= self.invest_increase_rate
        # print(f'assets_all: {self.assets_all}' )

    def adjust(self):
        diff = np.inf
        while abs(diff) > DELTA:
            diff = self.debt_all + self.equity_all - self.assets_all
            if diff > 0:
                self.balance_sheet.loc[0, '货币资金'] += diff
            if diff < 0:
                diff = abs(diff)
                self.balance_sheet.loc[0, '短期借款'] += diff
                # self.profit_statement.loc[0, '财务费用'] += diff * self.effective_interest_rate
                # print(f'net_profit is changed: {self.net_profit}')

            diff = self.debt_all + self.equity_all - self.assets_all

    @property
    def NOPAT(self):
        return self.profit_statement.loc[0, '营业收入'] - \
               np.sum(self.profit_statement.loc[0, ['营业成本', '营业税金及附加', '销售费用', '管理费用', '财务费用', '研发费用']])

    @property
    def FCF(self):
        return self.NOPAT \
               + (self.balance_sheet.loc[0, '递延所得税资产'] - self.base_company.balance_sheet.loc[0, '递延所得税资产']) \
               - (np.sum(self.balance_sheet.loc[0, cols_flow_operation_assets]) -
                  np.sum(self.base_company.balance_sheet.loc[0, cols_flow_operation_assets])) \
               - (self.balance_sheet.loc[0, '存货'] - self.base_company.balance_sheet.loc[0, '存货']) \
               + (np.sum(self.balance_sheet.loc[0, cols_flow_operation_debt]) -
                  np.sum(self.base_company.balance_sheet.loc[0, cols_flow_operation_debt])) \
               - (self.balance_sheet.loc[0, '固定资产净额'] - self.base_company.balance_sheet.loc[0, '固定资产净额'] +
                  self.balance_sheet.loc[0, '固定资产清理'] - self.base_company.balance_sheet.loc[0, '固定资产清理']) \
               - (self.balance_sheet.loc[0, '商誉'] - self.base_company.balance_sheet.loc[0, '商誉'])

    def build_cash_flow(self):
        pass


def df_process(df, table, code):
    df = df.drop(0, axis=0)
    df.index = df.iloc[:, 0]
    df = df.drop('报表日期', axis=1).T
    df = df.astype(float) / UNIT

    df['code'] = code
    df['date'] = df.index
    df['code_date'] = code + '_' + df.index

    df.reset_index(drop=True, inplace=True)

    df.to_sql(table, engine, if_exists='append',
              dtype={'code_date': CHAR(15), 'code': CHAR(6), 'date': CHAR(8), })
    return df


def from_sql(code, date, table):
    try:
        df = pd.read_sql(
            f'''select * from {table} 
                where code_date = "{code}_{date}"
            ''',
            engine)
        flag = True
    except:
        df = None
        flag = False
    return df, flag


def get_data(code, date):
    df_profit_statement, flag_profit_statement = from_sql(code, date, 'profit_statement')
    if not flag_profit_statement or len(df_profit_statement) < 1:
        df_profit_statement = ts.get_profit_statement(code)
        df_profit_statement = df_process(df_profit_statement, 'profit_statement', code)

    df_balance_sheet, flag_balance_sheet = from_sql(code, date, 'balance_sheet')
    if not flag_balance_sheet or len(df_balance_sheet) < 1:
        df_balance_sheet = ts.get_balance_sheet(code)
        df_balance_sheet = df_process(df_balance_sheet, 'balance_sheet', code)

    df_cash_flow, flag_cash_flow = from_sql(code, date, 'cash_flow')
    if not flag_cash_flow or len(df_cash_flow) < 1:
        df_cash_flow = ts.get_cash_flow(code)
        df_cash_flow = df_process(df_cash_flow, 'cash_flow', code)

    return df_profit_statement, df_balance_sheet, df_cash_flow


def dump_company(code, company_flow):
    df = pd.concat([c.balance_sheet.T for c in company_flow], axis=1)
    df.to_csv(f'data/{code}_balance_sheet.csv')
    print(f'dump balance sheet to : data/{code}_balance_sheet.csv')

    df = pd.concat([c.profit_statement.T for c in company_flow], axis=1)
    df.to_csv(f'data/{code}_profit_statement.csv')
    print(f'dump balance sheet to : data/{code}_profit_statement.csv')

    print('dump success!')


if __name__ == '__main__':
    code = '603288'
    date = '20181231'

    df_profit_statement, df_balance_sheet, df_cash_flow = get_data(code, date)
    sjf_20181231 = Company(df_profit_statement, df_balance_sheet, df_cash_flow, date)

    growth_rate = 1.16
    invest_increase_rate = 1.15
    sjf_2019_e = CompanyPredict(sjf_20181231, growth_rate, invest_increase_rate)
    print('done')

    sjf_2019_e.profit_statement.T.to_csv('profit_statement.csv')
    sjf_2019_e.balance_sheet.T.to_csv('balance_sheet.csv')

    sjf_20181231.profit_statement.T.to_csv('profit_statement1.csv')
    sjf_20181231.balance_sheet.T.to_csv('balance_sheet1.csv')
