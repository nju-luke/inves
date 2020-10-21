-- 各指标ttm分析
drop table if exists tmp_ttm;
create table tmp_ttm as
select ts_code,
    name,
    end_date,
    round(dt_roe_ttm, 2)                 dt_roe_ttm,
    round(netprofit_yoy_ttm / 4, 2)      netprofit_yoy_ttm,
    round(dt_netprofit_yoy_ttm / 4, 2)   dt_netprofit_yoy_ttm,
    round(grossprofit_margin_ttm / 4, 2) grossprofit_margin_ttm,
    round(netprofit_margin_ttm / 4, 2)   netprofit_margin_ttm
from (
    select ts_code,
        name,
        end_date,
            q_dt_roe
            + lead(q_dt_roe, 1) over (partition by ts_code order by end_date desc)
            + lead(q_dt_roe, 2) over (partition by ts_code order by end_date desc)
            + lead(q_dt_roe, 3) over (partition by ts_code order by end_date desc)         dt_roe_ttm,
            netprofit_yoy
            + lead(netprofit_yoy, 1) over (partition by ts_code order by end_date desc)
            + lead(netprofit_yoy, 2) over (partition by ts_code order by end_date desc)
            + lead(netprofit_yoy, 3) over (partition by ts_code order by end_date desc)    netprofit_yoy_ttm,
            dt_netprofit_yoy
            + lead(dt_netprofit_yoy, 1) over (partition by ts_code order by end_date desc)
            + lead(dt_netprofit_yoy, 2) over (partition by ts_code order by end_date desc)
            + lead(dt_netprofit_yoy, 3) over (partition by ts_code order by end_date desc) dt_netprofit_yoy_ttm,
            grossprofit_margin
            + lead(grossprofit_margin, 1) over (partition by ts_code order by end_date desc)
            + lead(grossprofit_margin, 2) over (partition by ts_code order by end_date desc)
            + lead(grossprofit_margin, 3) over (partition by ts_code order by end_date desc)
                                                                                           grossprofit_margin_ttm,
            netprofit_margin
            + lead(netprofit_margin, 1) over (partition by ts_code order by end_date desc)
            + lead(netprofit_margin, 2) over (partition by ts_code order by end_date desc)
            + lead(netprofit_margin, 3) over (partition by ts_code order by end_date desc) netprofit_margin_ttm
    from fina_indicator) A
where dt_roe_ttm is not null;


-- todo 求中位数，25、75分位数
-- 行业PE
drop table if exists pe_industry;
CREATE TABLE pe_industry AS
SELECT trade_date, A.industry, pe_ttm_ind, pe_ttm_ind_his
FROM (SELECT trade_date,
          industry,
          ROUND(AVG(pe_ttm), 2) pe_ttm_ind
      FROM stock_basic sb
               JOIN data_20200901 d ON sb.ts_code = d.ts_code
      WHERE d.trade_date = '20200901'
      GROUP BY industry) A
         JOIN
(SELECT industry,
     ROUND(AVG(pe_ttm)) pe_ttm_ind_his
 FROM stock_basic sb
          JOIN daily_basic db ON sb.ts_code = db.ts_code
 WHERE trade_date > REPLACE(DATE_SUB('20200901', INTERVAL 360 DAY), '-', '')
   AND trade_date > REPLACE(DATE_ADD(sb.list_date, INTERVAL 60 DAY), '-', '')
 GROUP BY industry) B ON A.industry = B.industry;


-- 个股PE
drop table if exists pe_history;
create table pe_history as
select sb.ts_code, sb.name, round(avg(pe), 2) pe_his, round(avg(pe_ttm), 2) pe_ttm_his
from stock_basic sb
         join daily_basic db on sb.ts_code = db.ts_code
where trade_date > replace(date_add(sb.list_date, INTERVAL 60 DAY), '-', '')
  and trade_date > '20180101'
group by sb.ts_code
;

-- 分析
drop table if exists tmp_results_20200901;
create table tmp_results_20200901 as
SELECT A.ts_code,
    sb.industry,
    sb.name,
    dt_roe_ttm,
    avg_roe,
    pe,
    pe_ttm,
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
    SELECT name, round(AVG(dt_roe_ttm), 2) AS avg_roe
    FROM tmp_ttm
    where end_date > '20191231'
    GROUP BY name
) B ON A.name = B.name
         JOIN data_20200901 C ON A.ts_code = C.ts_code
         join stock_basic sb on A.ts_code = sb.ts_code
         join pe_industry pi on sb.industry = pi.industry
         join pe_history ph on A.ts_code = ph.ts_code
WHERE rn = 1
  and dt_roe_ttm > 20
  AND avg_roe > 20
  AND pe < 50
  AND pe_ttm < 38
  AND dt_netprofit_yoy_ttm > 10
ORDER BY dt_roe_ttm DESC;



select *
from tmp_results_20200710;

-- 市值增长比率
select base.ts_code, f_tm / total_mv
from (
    select ts_code, total_mv
    from daily_basic
    where ts_code in (select distinct ts_code from tmp_results)
      and trade_date = '20190430'
) base
         join (
    select ts_code, max(total_mv) f_tm
    from daily_basic
    where ts_code in (select distinct ts_code from tmp_results)
      and trade_date > '20190430'
      and trade_date < replace(date_add('20190430', interval 360 day), "-", "")
    group by ts_code
) F
              on base.ts_code = F.ts_code;


-- 删除数据
delete
from daily_basic
where trade_date > '20200707';

select count(1)
from daily_basic
where trade_date = '20200709'



select tr.*, f_tm / total_mv y
from (
    select *
    from daily_basic
    where ts_code in (select distinct ts_code from tmp_results)
      and trade_date = '20200710'
) base
         join (
    select ts_code, max(total_mv) f_tm
    from daily_basic
    where ts_code in (select distinct ts_code from tmp_results)
      and trade_date > '20200710'
      and trade_date < replace(date_add('20200710', interval 90 day), "-", "")
    group by ts_code
) F on base.ts_code = F.ts_code
         join stock_basic sb
              on base.ts_code = sb.ts_code
         join tmp_results tr on base.ts_code = tr.ts_code;


select *
from fina_indicator
where name = '姚记科技';


-- 更新收益表
select name, `industry`
from stock_basic
where name in
      ('泸州老窖', '古井贡酒', '西王食品', '黑芝麻', '燕京啤酒', '*ST西发', '酒鬼酒', '承德露露', '五粮液', '顺鑫农业', '张裕A', '双汇发展', '兰州黄河', '三全食品',
       '洋河股份', '皇氏集团', '得利斯', '珠江啤酒', '双塔食品', '佳隆股份', '涪陵榨菜', '金字火腿', '胜景山河(I', '洽洽食品', '百润股份', '贝因美', '好想你', '青青稞酒',
       'ST加加', '克明面业', '煌上煌', '海欣食品', '*ST麦趣', '龙大肉食', '燕塘乳业', 'ST科迪', '桂发祥', '华统股份', '盐津铺子', '庄园牧场', '新乳业', '西麦食品',
       '甘源食品', '汤臣倍健', '华宝股份', '三只松鼠', '仙乐健康', '科拓生物', '古越龙山', '上海梅林', 'ST中葡', '重庆啤酒', '莲花健康', '吉林森工', '伊力特', '金种子酒',
       'ST椰岛', '维维股份', '恒顺醋业', '通葡股份', '天润乳业', '三元股份', '贵州茅台', '莫高股份', '老白干酒', '惠泉啤酒', '光明乳业', '青岛啤酒', '金枫酒业', '舍得酒业',
       '水井坊', '山西汾酒', '星湖科技', '中炬高新', '妙可蓝多', '伊利股份', '会稽山', '爱普股份', '千禾味业', '广州酒家', '养元饮品', '迎驾贡酒', '海天味业', '天味食品',
       '安井食品', '今世缘', '绝味食品', '惠发食品', '口子窖', '安记食品', '有友食品', '香飘飘', '良品铺子', '日辰股份', '来伊份', 'ST威龙', '桃李面包', '元祖股份',
       '金徽酒', '均瑶健康', '嘉必优')
order by industry


select d.ts_code, name, industry, pe, pb
from stock_basic
         join data_20200812 d on stock_basic.ts_code = d.ts_code
where industry = "互联网"


select pe
from daily_basic
where ts_code = '600519.sz'
# where name='洋河股份'


## ROE FCF
select fi.ts_code, name, roe_dt, close, pe_ttm, close1
from fina_indicator fi
         join
(select ts_code
 from fina_indicator
 where end_date = '20171231'
   and netprofit_margin > 10
 order by roa desc
 limit 200) A
on fi.ts_code = A.ts_code
         join (select ts_code, close, pe_ttm
               from data_20190430
               where pe_ttm < 30
                 and total_mv > 1000000
) db on fi.ts_code = db.ts_code
         join (select ts_code, close close1 from data_20200820) db1 on fi.ts_code = db1.ts_code
where end_date = '20181231'
  and netprofit_margin > 10
order by roa desc
limit 20




