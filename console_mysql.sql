-- roe 分析
drop table if exists tmp_roe_ttm;
create table tmp_roe_ttm as
select ts_code, name, end_date, round(dt_roe_ttm, 2) dt_roe_ttm
from (
    select ts_code,
        name,
        code_date,
        end_date,
            q_dt_roe
            + lead(q_dt_roe, 1) over (partition by ts_code order by code_date desc)
            + lead(q_dt_roe, 2) over (partition by ts_code order by code_date desc)
            + lead(q_dt_roe, 3) over (partition by ts_code order by code_date desc) dt_roe_ttm
    from fina_indicator) A
where dt_roe_ttm is not null;



select A.name, dt_roe_ttm, avg_roe, pe, pe_ttm
from (
    select *,
        row_number() over (partition by name order by name) rn
    from tmp_roe_ttm
) A
     join(
    select name, avg(dt_roe_ttm) avg_roe
    from tmp_roe_ttm
    group by name
) B on A.name = B.name
     join data_20200706 C on A.ts_code = C.ts_code
where rn = 1
  and dt_roe_ttm > 20
  and avg_roe > 20
  and pe < 50
  and pe_ttm < 38
order by dt_roe_ttm desc;


-- 行业PE
select avg(pe_ttm)
from stocks.data_20200708 dl
     join (
    select *
    from stocks.stock_basic
    where industry in (select industry from stocks.stock_basic where name = '水井坊')) bas
          on dl.ts_code = bas.ts_code;

-- 个股PE
-- list_date < today() - 3y
-- trade_date > today() - 2y
select avg(pe),avg(pe_ttm) from stocks.daily_basic
where ts_code='002863.SZ'
order by trade_date;



-- 增长
select name,
    end_date,
    netprofit_yoy,
    tr_yoy,
    or_yoy,
    q_npta
from fina_indicator
where name = '金溢科技'



select name,
    end_date,
    profit_dedt / 1e6,
    netprofit_yoy,
    dt_netprofit_yoy
from fina_indicator
where name = "金溢科技"

select *
from stock_basic
where name = "金溢科技"

select code_date,
    count(1) cnt
from daily_basic
group by code_date
having cnt > 1

select *
from tmp_roe_ttm
where name = '南华仪器'



select *
from daily_basic
where ts_code = '000538.SZ';

select count(distinct ts_code)
from daily_basic


select *
from