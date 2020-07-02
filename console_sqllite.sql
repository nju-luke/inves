create table test
(
    name  string,
    age   int,
    score int
);

select *
from test;


-- 持续收益较高的
select code, name, count(1) cnt
from profit_data
where gross_profit_rate > 60
  and net_profit_ratio > 20
  and roe > 12
  and year >= 2015
group by code, name
order by cnt desc
limit 100;

-- 对应市盈率、收益等情况
select ctb.*,
       ta.per,
       avm,
       mbrg,
       nprg,
       year,
       pd.*
from today_all ta
         join(
    select code, name, count(1) cnt
    from profit_data
    where gross_profit_rate > 60
      and net_profit_ratio > 20
      and roe > 12
      and year >= 2015
    group by code, name
    order by cnt desc
    limit 100
) ctb on ta.code = ctb.code
         join(
    select roe, net_profit_ratio, gross_profit_rate, net_profits, code
    from profit_data
    where year = 2019
      and quarter = 4
) pd on pd.code = ctb.code
         join(select growth_data.code, mbrg, nprg, avm, year
              from growth_data
                       join (select code, avg(mbrg) avm
                             from growth_data
                             group by code
                             having avm > 5
              ) tmp
                            on growth_data.code = tmp.code
) gd on gd.code = ctb.code
where per > 8
  and per < 50
order by per, cnt desc, roe desc, year;



--- 行业分析
-- 白酒
create table tmp_profit_data as
select *
from profit_data
where code in
      ('600519', '000858', '002304', '600809', '000568', '000596', '603369', '000869', '603589', '603198', '600559',
       '600779', '600702', '603919', '000799', '600197')
;

select code,name,per from today_all
where code in
      ('000729',
'000929',
'002461',
'002646',
'600059',
'600132',
'600199',
'600573',
'600600',
'600616',
'601579');

-- 行业内营收占比
select y2019.code, y2019.name,
       y2016.bsi_ratio,
       y2017.bsi_ratio,
       y2018.bsi_ratio,
       y2019.bsi_ratio
from (
         select code, name, business_income / bis bsi_ratio
         from tmp_profit_data A
                  join (select sum(business_income) bis from tmp_profit_data where year = 2019) B
         where year = 2019
         order by business_income desc
     ) y2019
         join
     (
         select code, name, business_income / bis bsi_ratio
         from tmp_profit_data A
                  join (select sum(business_income) bis from tmp_profit_data where year = 2018) B
         where year = 2018
         order by business_income desc
     ) y2018 on y2019.code=y2018.code
         join
     (
         select code, name, business_income / bis bsi_ratio
         from tmp_profit_data A
                  join (select sum(business_income) bis from tmp_profit_data where year = 2017) B
         where year = 2017
         order by business_income desc
     ) y2017 on y2019.code=y2017.code
         join
     (
         select code, name, business_income / bis bsi_ratio
         from tmp_profit_data A
                  join (select sum(business_income) bis from tmp_profit_data where year = 2016) B
         where year = 2016
         order by business_income desc
     ) y2016 on y2019.code=y2016.code
order by y2019.bsi_ratio desc;


-- 毛利率/净利率变化
select y2019.code, y2019.name,
       y2016.gross_profit_rate,
       y2017.gross_profit_rate,
       y2018.gross_profit_rate,
       y2019.gross_profit_rate,

       y2019.name,
       y2016.net_profit_ratio,
       y2017.net_profit_ratio,
       y2018.net_profit_ratio,
       y2019.net_profit_ratio

from tmp_profit_data y2019
join tmp_profit_data y2018
on y2019.code=y2018.code
join tmp_profit_data y2017
on y2019.code=y2017.code
join tmp_profit_data y2016
on y2019.code=y2016.code
where y2019.year=2019 and
      y2018.year=2018 and
      y2017.year=2017 and
      y2016.year=2016
order by y2019.business_income desc


