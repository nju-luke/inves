-- 各指标ttm分析
drop table if exists tmp_ttm;
create table tmp_ttm as
select ts_code, name, end_date, round(dt_roe_ttm, 2) dt_roe_ttm,
	round(netprofit_yoy_ttm/4,2) netprofit_yoy_ttm,
    round(dt_netprofit_yoy_ttm/4,2) dt_netprofit_yoy_ttm,
    round(grossprofit_margin_ttm/4,2) grossprofit_margin_ttm,
    round(netprofit_margin_ttm/4,2) netprofit_margin_ttm
from (
         select ts_code,
                name,
                end_date,
                q_dt_roe
                    + lead(q_dt_roe, 1) over (partition by ts_code order by end_date desc)
                    + lead(q_dt_roe, 2) over (partition by ts_code order by end_date desc)
                    + lead(q_dt_roe, 3) over (partition by ts_code order by end_date desc) dt_roe_ttm,
				netprofit_yoy
					+ lead(netprofit_yoy, 1) over (partition by ts_code order by end_date desc)
					+ lead(netprofit_yoy, 2) over (partition by ts_code order by end_date desc)
					+ lead(netprofit_yoy, 3) over (partition by ts_code order by end_date desc) netprofit_yoy_ttm,
				dt_netprofit_yoy
					+ lead(dt_netprofit_yoy, 1) over (partition by ts_code order by end_date desc)
					+ lead(dt_netprofit_yoy, 2) over (partition by ts_code order by end_date desc)
					+ lead(dt_netprofit_yoy, 3) over (partition by ts_code order by end_date desc) dt_netprofit_yoy_ttm,
				grossprofit_margin
					+ lead(grossprofit_margin, 1) over (partition by ts_code order by end_date desc)
					+ lead(grossprofit_margin, 2) over (partition by ts_code order by end_date desc)
					+ lead(grossprofit_margin, 3) over (partition by ts_code order by end_date desc) grossprofit_margin_ttm,
				netprofit_margin
					+ lead(netprofit_margin, 1) over (partition by ts_code order by end_date desc)
					+ lead(netprofit_margin, 2) over (partition by ts_code order by end_date desc)
					+ lead(netprofit_margin, 3) over (partition by ts_code order by end_date desc) netprofit_margin_ttm
				 from fina_indicator) A
where dt_roe_ttm is not null;

-- 行业PE
drop table if exists  pe_industry;
create table pe_industry as
select industry, avg(pe_ttm) pe_ttm_ind from stock_basic sb
join data_20200709 d on sb.ts_code = d.ts_code
group by industry;

-- 个股PE
drop table if exists pe_history;
create table pe_history as
select ts_code, avg(pe) pe_his, avg(pe_ttm) pe_ttm_his
from daily_basic
where trade_date>='20180101'
group by ts_code;


-- 分析
SELECT A.name, dt_roe_ttm, avg_roe, pe, pe_ttm,
       pe_ttm_ind,
       pe_his,
       pe_ttm_his,
       netprofit_yoy_ttm,
       dt_netprofit_yoy_ttm,
       grossprofit_margin_ttm,
       netprofit_margin_ttm
FROM (
	SELECT *, row_number() over (partition by ts_code order by end_date desc) rn
	FROM tmp_ttm) A
JOIN (
		SELECT name, AVG(dt_roe_ttm) AS avg_roe
		FROM tmp_ttm
		where end_date > '20181231'
		GROUP BY name
	) B ON A.name = B.name
JOIN data_20200709 C ON A.ts_code = C.ts_code
join stock_basic sb on A.ts_code = sb.ts_code
join pe_industry pi on sb.industry = pi.industry
join pe_history ph on A.ts_code = ph.ts_code
WHERE rn = 1
    and dt_roe_ttm > 20
	AND avg_roe > 20
	AND pe < 50
	AND pe_ttm < 38
ORDER BY dt_roe_ttm DESC;


select name, count(1) cnt from tmp_ttm
group by name
having cnt>1;


-- 当前公司信息
select d.ts_code, name, industry, area, close, pe_ttm, total_mv from stock_basic sb
join data_20200709 d on sb.ts_code=d.ts_code
where name='洋河股份';



-- 行业PE
select industry,avg(pe_ttm)
from stocks.data_20200709 dl
join (
	select *
	from stocks.stock_basic
	where industry in (select industry from stocks.stock_basic where name = '洋河股份')
    ) bas
  on dl.ts_code = bas.ts_code;

-- 个股PE
-- list_date < today() - 3y
-- trade_date > today() - 2y
select ts_code,
       avg(pe),avg(pe_ttm)
from stocks.daily_basic
where ts_code='002304.SZ'
and trade_date>='20180101'
group by ts_code
;



select 	ts_code, end_date,
       netprofit_yoy_ttm,
       dt_netprofit_yoy_ttm,
       grossprofit_margin_ttm,
       netprofit_margin_ttm
from tmp_ttm
where ts_code='002304.SZ';

-- 增长
select A.*, B.pe_ttm, B.total_mv from (
select ts_code,
       name,
	netprofit_yoy_ttm,
	dt_netprofit_yoy_ttm,
    grossprofit_margin_ttm,
    netprofit_margin_ttm
 from tmp_ttm
 where end_date='20200331'
	 and netprofit_yoy_ttm > 10 	-- 净利润增长
     and dt_netprofit_yoy_ttm > 10	-- 扣非净利润增长
	 and netprofit_margin_ttm > 10 	-- 净利率
     and grossprofit_margin_ttm > 30
    ) A
join data_20200709 B on A.ts_code=B.ts_code
order by dt_netprofit_yoy_ttm desc
;


-- 毛利
select  ts_code,name, end_date,
	grossprofit_margin_ttm ,
    netprofit_margin_ttm
from tmp_ttm;


select count(distinct ts_code) from tmp_ttm



select code_date,count(1) cnt from fina_indicator
group by code_date
having cnt >1
