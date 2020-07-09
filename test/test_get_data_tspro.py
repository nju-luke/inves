# -*- coding:utf-8 -*-
"""
author: byangg
datettime: 7/9/2020 10:34
"""
from unittest import TestCase

from get_data_tspro import download_fina_indicator_all, get_fina_indicator, get_fina_mainbz, \
    download_fina_mainbz_all, get_daily_basic_all, get_daily_code_date, get_daily_basic_by_date

TS_CODE = "002027.SZ"
DATE = "20200708"

class TestGetDataTspro(TestCase):
    def test_get_fina_indicator(self):
        get_fina_indicator("002027.SZ")

    def test_get_fina_mainbz(self):
        get_fina_mainbz(TS_CODE)

    def test_get_daily_code_date(self):
        get_daily_code_date(TS_CODE)

    def test_get_daily_by_date(self):
        get_daily_basic_by_date()
