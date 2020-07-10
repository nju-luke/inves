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
select trade_date, industry, round(avg(pe_ttm),2) pe_ttm_ind from stock_basic sb
join data_20200710 d on sb.ts_code = d.ts_code
group by industry;

	-- 历史行业PE
	create table pe_industry as
	select trade_date, industry, round(avg(pe_ttm),2) pe_ttm_ind from stock_basic sb
	join daily_basic d on sb.ts_code = d.ts_code
	where d.trade_date='20200710'
	group by industry;


-- 个股PE
drop table if exists pe_history;
create table pe_history as
select sb.ts_code, sb.name, round(avg(pe),2) pe_his, round(avg(pe_ttm),2) pe_ttm_his 
from stock_basic sb
join daily_basic db on sb.ts_code=db.ts_code
where trade_date > replace(date_add(sb.list_date , INTERVAL 60 DAY),'-','')
and trade_date>'20180101'
group by sb.ts_code
;

-- 分析
drop table if exists tmp_results_20200710;
create table tmp_results_20200710 as
SELECT ts_code, sb.industry, sb.name, 
	   dt_roe_ttm, 
       avg_roe, 
       pe, pe_ttm,
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
		SELECT name, round(AVG(dt_roe_ttm),2) AS avg_roe
		FROM tmp_ttm
		where end_date > '20181231'
		GROUP BY name
	) B ON A.name = B.name
JOIN data_20200710 C ON A.ts_code = C.ts_code
join stock_basic sb on A.ts_code = sb.ts_code
join pe_industry pi on sb.industry = pi.industry
join pe_history ph on A.ts_code = ph.ts_code
WHERE rn = 1
    and dt_roe_ttm > 20
	AND avg_roe > 20
	AND pe < 50
	AND pe_ttm < 38
ORDER BY dt_roe_ttm DESC;

select * from tmp_results_20200710;

-- 市值增长比率
select base.ts_code, f_tm/total_mv 
from(
	select ts_code, total_mv 
    from daily_basic
	where ts_code in (select distinct ts_code from tmp_results) 
		and trade_date='20190430' 
) base
join (
	select ts_code, max(total_mv) f_tm from daily_basic
    where ts_code in (select distinct ts_code from tmp_results) 
		and trade_date > '20190430' and trade_date < replace(date_add('20190430', interval 360 day),"-","")
	group by ts_code
) F
on base.ts_code=F.ts_code;




