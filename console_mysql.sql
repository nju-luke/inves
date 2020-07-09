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


-- roe 分析
SELECT A.name, dt_roe_ttm, avg_roe, pe, pe_ttm
FROM (
	SELECT *, row_number() OVER (PARTITION BY name ORDER BY name) AS rn
	FROM tmp_ttm
) A
	JOIN (
		SELECT name, AVG(dt_roe_ttm) AS avg_roe
		FROM tmp_ttm
		GROUP BY name
	) B
	ON A.name = B.name
	JOIN data_20200708 C ON A.ts_code = C.ts_code
WHERE rn = 1
	AND dt_roe_ttm > 20
	AND avg_roe > 20
	AND pe < 50
	AND pe_ttm < 38
ORDER BY dt_roe_ttm DESC;


-- 行业PE
select avg(pe_ttm)
from stocks.data_20200708 dl
join (
	select *
	from stocks.stock_basic
	where industry in (select industry from stocks.stock_basic where name = '水井坊')
    ) bas
  on dl.ts_code = bas.ts_code;

-- PE
-- list_date < today() - 3y
-- trade_date > today() - 2y
select avg(pe),avg(pe_ttm)
from stocks.daily_basic
where ts_code='002863.SZ'
and trade_date>='20190101';



-- 增长
select name,
	netprofit_yoy_ttm,
	dt_netprofit_yoy_ttm,
    grossprofit_margin_ttm,
    netprofit_margin_ttm
 from tmp_ttm
 where end_date='20181231'
	 and netprofit_yoy_ttm > 10 	-- 净利润增长
     and dt_netprofit_yoy_ttm > 10	-- 扣非净利润增长
	 and netprofit_margin_ttm > 10 	-- 净利率
 order by dt_netprofit_yoy_ttm desc
;


-- 毛利
select  ts_code,name, end_date,
	grossprofit_margin_ttm ,
    netprofit_margin_ttm
from tmp_ttm;


select count(distinct ts_code) from tmp_ttm




