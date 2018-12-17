with existingpipe AS
    (SELECT *
   FROM (WITH opendeal AS
  (SELECT *
   FROM (WITH datespine AS
  (SELECT a.ddate,
          last_day(a.ddate,'quarter') AS quarter
   FROM midaxo.dev.datetable_clean a
   WHERE a.ddate > dateadd('quarter',-5,dateadd('day',1,last_day(current_date,'quarter')))),
     opendeals AS
  (SELECT DISTINCT d.ddate,
                   a.deal_id,
                   a.pipeline_stage,
                   a.deal_amount
   FROM datespine d
   left join MIDAXO.DEV.DEAL_ARCHIVE_CLEAN a
     ON d.ddate between a.validfrom AND a.validto
   WHERE last_day(a.closedate, 'quarter') = last_day(d.ddate,'quarter')
     AND last_day(a.createdate,'quarter') < last_day(d.ddate,'quarter')
     AND a.pipeline_type = 'direct'
     AND a.pipeline_stage not in ('activation',
                                  'nurture',
                                  'qualification',
                                  'closed won',
                                  'closed lost'))
SELECT DISTINCT a.ddate,
                a.pipeline_stage,
                count(a.deal_amount) over (partition BY a.ddate, a.pipeline_stage) AS dealcount
FROM opendeals a
ORDER BY a.ddate desc
) AS forecast_open_deals),
     winrate AS
  (SELECT *
   FROM (WITH datespine AS
  (SELECT a.ddate,
          last_day(a.ddate,'quarter') AS quarter,
          to_number(extract('quarter', a.ddate)) AS qr,
          to_number(extract('month', a.ddate)) AS mo,
          (CASE
               WHEN qr = 1 THEN mo
               WHEN qr = 2 THEN mo - 3
               WHEN qr = 3 THEN mo - 6
               WHEN qr = 4 THEN mo - 9
           END) AS quartermonth
   FROM midaxo.dev.datetable_clean a
   WHERE a.ddate between dateadd('quarter',-10,dateadd('day',1,last_day(current_date,'quarter'))) AND last_day(current_date,'quarter')),
     currentdeal AS
  (SELECT a.deal_id,
          a.closedate,
          a.pipeline_stage
   FROM midaxo.dev.deal a
   WHERE a.pipeline_type = 'direct'),
     dedupe AS
  (SELECT DISTINCT last_day(d.ddate,'month') AS mo,
                   d.quartermonth,
                   a.deal_id,
                   a.pipeline_stage,
                   CASE
                       WHEN last_day(c.closedate,'quarter') <> last_day(a.closedate,'quarter') then 'pushed'
                       WHEN c.pipeline_stage = 'closed won' then 'won'
                       WHEN c.pipeline_stage = 'closed lost' then 'lost'
                       ELSE 'open'
                   END AS status
   FROM datespine d
   left join MIDAXO.DEV.DEAL_ARCHIVE_CLEAN a
     ON d.ddate between a.validfrom AND a.validto
   left join currentdeal c
     ON c.deal_id = a.deal_id
     WHERE a.pipeline_type = 'direct'
       AND status <> 'open'
       AND a.pipeline_stage not in ('activation',
                                    'nurture',
                                    'qualification',
                                    'closed won',
                                    'closed lost')
       AND last_day(a.closedate, 'quarter') = last_day(d.ddate,'quarter')
       AND last_day(a.createdate,'quarter') < last_day(d.ddate,'quarter')),
     dealstatus AS
    (SELECT d.mo,
            d.quartermonth,
            d.pipeline_stage,
            sum(CASE
                    WHEN d.status = 'won' then 1
                    ELSE 0
                END) AS dwin,
            sum(CASE
                    WHEN d.status = 'pushed' then 1
                    ELSE 0
                END) AS dpush,
            sum(CASE
                    WHEN d.status = 'lost' then 1
                    ELSE 0
                END) AS dlost,
            count(deal_id) AS dtotal
   FROM dedupe d
   GROUP BY mo,
            d.quartermonth,
            d.pipeline_stage)
SELECT distinct d.ddate,
       s.pipeline_stage,
       sum(s.dwin) over (partition by d.ddate, s.pipeline_stage) as win,
       sum(s.dpush) over (partition by d.ddate, s.pipeline_stage) as push,
       sum(s.dlost) over (partition by d.ddate, s.pipeline_stage) as lost,
       sum(s.dtotal) over (partition by d.ddate, s.pipeline_stage) as total,
       win/total AS real_win_rate,
       push/total AS push_rate
FROM datespine d
left join dealstatus s
  ON s.quartermonth = d.quartermonth
  AND s.mo between dateadd('quarter',-4,last_day(d.ddate,'quarter')) AND dateadd('quarter',-1,last_day(d.ddate,'quarter'))
WHERE d.ddate between dateadd('quarter',-5,dateadd('day',1,last_day(current_date,'quarter'))) AND last_day(current_date,'quarter')
) AS forecast_real_win_rate),
     wondeal AS
  (SELECT *
   FROM (WITH datespine AS
  (SELECT b.ddate,
          last_day(b.ddate,'quarter') AS quarter
   FROM midaxo.dev.datetable_clean b
   WHERE b.ddate > dateadd('quarter',-5,dateadd('day',1,last_day(current_date,'quarter')))),
     deal AS
  (SELECT b.closedate,
          b.createdate,
          b.deal_id,
          b.pipeline_stage,
          b.deal_amount
   FROM midaxo.dev.deal b
   WHERE b.pipeline_stage ='closed won'
     AND last_day(b.createdate,'quarter') <= last_day(b.closedate,'quarter')
     AND b.pipeline_type = 'direct'),
     wondeals AS
  (SELECT d.ddate,
          b.deal_id,
          CASE
              WHEN last_day(b.createdate,'quarter') = last_day(b.closedate,'quarter') then 'create_close'
              WHEN last_day(b.createdate,'quarter') < last_day(b.closedate,'quarter') then 'existing'
              ELSE 'other'
          END AS status,
          b.deal_amount
   FROM datespine d
   left join deal b
     ON d.ddate>=b.closedate
     AND last_day(b.closedate,'quarter')=last_day(d.ddate,'quarter'))
SELECT DISTINCT b.ddate,
                b.status,
                count(b.deal_id) over (partition BY b.ddate, b.status) AS dealcount
FROM wondeals b
) AS forecast_won_deals
   where status = 'existing'),
     forecast AS
  (SELECT distinct d.ddate,
          max(x.dealcount) over (partition by d.ddate) as won,
          sum(d.dealcount * w.real_win_rate) over (partition by d.ddate) AS existing,
          existing + ifnull(won,0) as forecast
   FROM opendeal d
   left join winrate w
     ON w.ddate = d.ddate
     AND d.pipeline_stage = w.pipeline_stage
   left join wondeal x
    on x.ddate = d.ddate)

SELECT distinct
f.ddate,
sum(f.forecast) over (partition by f.ddate) as forecast,
w.dealcount as actual,
forecast/actual - 1 as forecast_error
FROM forecast f
left join wondeal w
  on w.ddate = last_day(f.ddate,'quarter')
WHERE f.ddate <= current_date
--  AND date_part('day',f.ddate) = 5
ORDER BY f.ddate DESC
) AS forecast_existing_pipe),
     createplug AS
  (SELECT *
   FROM (with cc AS
    (SELECT *
   FROM (with datespine AS
  (SELECT a.ddate,
          last_day(a.ddate,'quarter') AS quarter
   FROM midaxo.dev.datetable_clean a
   WHERE a.ddate > dateadd('quarter',-5,dateadd('day',1,last_day(current_date,'quarter')))),
     deal AS
  (SELECT a.deal_id,
          a.createdate,
          a.closedate,
          a.deal_amount,
          CASE
              WHEN last_day(a.createdate, 'quarter') = last_day(a.closedate, 'quarter') then true
              ELSE false
          END AS create_in_quarter
   FROM MIDAXO.DEV.DEAL a
   WHERE a.pipeline_type = 'direct'
     AND a.pipeline_stage = 'closed won'
     AND createdate < closedate),
     createclose AS
  (SELECT DISTINCT last_day(d.closedate,'quarter') AS quarter,
                   sum(CASE
                           WHEN d.create_in_quarter = true then 1
                           ELSE 0
                       END) over (partition BY quarter) AS create_close,
                   count(d.deal_id) over (partition BY quarter) AS total,
                   create_close/total AS create_close_rate
   FROM deal d)
SELECT distinct d.ddate,
                sum(c.create_close)/sum(c.total) AS forecast
FROM datespine d
left join createclose c
  ON c.quarter between dateadd('day',1,last_day(dateadd('quarter',-3,d.ddate),'quarter')) AND last_day(dateadd('quarter',-1,d.ddate),'quarter')
GROUP BY d.ddate
ORDER BY d.ddate DESC
) AS forecast_create_plug),
     ef AS
  (SELECT *
   FROM (WITH opendeal AS
  (SELECT *
   FROM (WITH datespine AS
  (SELECT a.ddate,
          last_day(a.ddate,'quarter') AS quarter
   FROM midaxo.dev.datetable_clean a
   WHERE a.ddate > dateadd('quarter',-5,dateadd('day',1,last_day(current_date,'quarter')))),
     opendeals AS
  (SELECT DISTINCT d.ddate,
                   a.deal_id,
                   a.pipeline_stage,
                   a.deal_amount
   FROM datespine d
   left join MIDAXO.DEV.DEAL_ARCHIVE_CLEAN a
     ON d.ddate between a.validfrom AND a.validto
   WHERE last_day(a.closedate, 'quarter') = last_day(d.ddate,'quarter')
     AND last_day(a.createdate,'quarter') < last_day(d.ddate,'quarter')
     AND a.pipeline_type = 'direct'
     AND a.pipeline_stage not in ('activation',
                                  'nurture',
                                  'qualification',
                                  'closed won',
                                  'closed lost'))
SELECT DISTINCT a.ddate,
                a.pipeline_stage,
                count(a.deal_amount) over (partition BY a.ddate, a.pipeline_stage) AS dealcount
FROM opendeals a
ORDER BY a.ddate desc
) AS forecast_open_deals),
     winrate AS
  (SELECT *
   FROM (WITH datespine AS
  (SELECT a.ddate,
          last_day(a.ddate,'quarter') AS quarter,
          to_number(extract('quarter', a.ddate)) AS qr,
          to_number(extract('month', a.ddate)) AS mo,
          (CASE
               WHEN qr = 1 THEN mo
               WHEN qr = 2 THEN mo - 3
               WHEN qr = 3 THEN mo - 6
               WHEN qr = 4 THEN mo - 9
           END) AS quartermonth
   FROM midaxo.dev.datetable_clean a
   WHERE a.ddate between dateadd('quarter',-10,dateadd('day',1,last_day(current_date,'quarter'))) AND last_day(current_date,'quarter')),
     currentdeal AS
  (SELECT a.deal_id,
          a.closedate,
          a.pipeline_stage
   FROM midaxo.dev.deal a
   WHERE a.pipeline_type = 'direct'),
     dedupe AS
  (SELECT DISTINCT last_day(d.ddate,'month') AS mo,
                   d.quartermonth,
                   a.deal_id,
                   a.pipeline_stage,
                   CASE
                       WHEN last_day(c.closedate,'quarter') <> last_day(a.closedate,'quarter') then 'pushed'
                       WHEN c.pipeline_stage = 'closed won' then 'won'
                       WHEN c.pipeline_stage = 'closed lost' then 'lost'
                       ELSE 'open'
                   END AS status
   FROM datespine d
   left join MIDAXO.DEV.DEAL_ARCHIVE_CLEAN a
     ON d.ddate between a.validfrom AND a.validto
   left join currentdeal c
     ON c.deal_id = a.deal_id
     WHERE a.pipeline_type = 'direct'
       AND status <> 'open'
       AND a.pipeline_stage not in ('activation',
                                    'nurture',
                                    'qualification',
                                    'closed won',
                                    'closed lost')
       AND last_day(a.closedate, 'quarter') = last_day(d.ddate,'quarter')
       AND last_day(a.createdate,'quarter') < last_day(d.ddate,'quarter')),
     dealstatus AS
    (SELECT d.mo,
            d.quartermonth,
            d.pipeline_stage,
            sum(CASE
                    WHEN d.status = 'won' then 1
                    ELSE 0
                END) AS dwin,
            sum(CASE
                    WHEN d.status = 'pushed' then 1
                    ELSE 0
                END) AS dpush,
            sum(CASE
                    WHEN d.status = 'lost' then 1
                    ELSE 0
                END) AS dlost,
            count(deal_id) AS dtotal
   FROM dedupe d
   GROUP BY mo,
            d.quartermonth,
            d.pipeline_stage)
SELECT distinct d.ddate,
       s.pipeline_stage,
       sum(s.dwin) over (partition by d.ddate, s.pipeline_stage) as win,
       sum(s.dpush) over (partition by d.ddate, s.pipeline_stage) as push,
       sum(s.dlost) over (partition by d.ddate, s.pipeline_stage) as lost,
       sum(s.dtotal) over (partition by d.ddate, s.pipeline_stage) as total,
       win/total AS real_win_rate,
       push/total AS push_rate
FROM datespine d
left join dealstatus s
  ON s.quartermonth = d.quartermonth
  AND s.mo between dateadd('quarter',-4,last_day(d.ddate,'quarter')) AND dateadd('quarter',-1,last_day(d.ddate,'quarter'))
WHERE d.ddate between dateadd('quarter',-5,dateadd('day',1,last_day(current_date,'quarter'))) AND last_day(current_date,'quarter')
) AS forecast_real_win_rate),
     wondeal AS
  (SELECT *
   FROM (WITH datespine AS
  (SELECT b.ddate,
          last_day(b.ddate,'quarter') AS quarter
   FROM midaxo.dev.datetable_clean b
   WHERE b.ddate > dateadd('quarter',-5,dateadd('day',1,last_day(current_date,'quarter')))),
     deal AS
  (SELECT b.closedate,
          b.createdate,
          b.deal_id,
          b.pipeline_stage,
          b.deal_amount
   FROM midaxo.dev.deal b
   WHERE b.pipeline_stage ='closed won'
     AND last_day(b.createdate,'quarter') <= last_day(b.closedate,'quarter')
     AND b.pipeline_type = 'direct'),
     wondeals AS
  (SELECT d.ddate,
          b.deal_id,
          CASE
              WHEN last_day(b.createdate,'quarter') = last_day(b.closedate,'quarter') then 'create_close'
              WHEN last_day(b.createdate,'quarter') < last_day(b.closedate,'quarter') then 'existing'
              ELSE 'other'
          END AS status,
          b.deal_amount
   FROM datespine d
   left join deal b
     ON d.ddate>=b.closedate
     AND last_day(b.closedate,'quarter')=last_day(d.ddate,'quarter'))
SELECT DISTINCT b.ddate,
                b.status,
                count(b.deal_id) over (partition BY b.ddate, b.status) AS dealcount
FROM wondeals b

) AS forecast_won_deals
   where status = 'existing'),
     forecast AS
  (SELECT distinct d.ddate,
          max(x.dealcount) over (partition by d.ddate) as won,
          sum(d.dealcount * w.real_win_rate) over (partition by d.ddate) AS existing,
          existing + ifnull(won,0) as forecast
   FROM opendeal d
   left join winrate w
     ON w.ddate = d.ddate
     AND d.pipeline_stage = w.pipeline_stage
   left join wondeal x
    on x.ddate = d.ddate)

SELECT distinct
f.ddate,
sum(f.forecast) over (partition by f.ddate) as forecast,
w.dealcount as actual,
forecast/actual - 1 as forecast_error
FROM forecast f
left join wondeal w
  on w.ddate = last_day(f.ddate,'quarter')
WHERE f.ddate <= current_date
--  AND date_part('day',f.ddate) = 5
ORDER BY f.ddate DESC
) AS forecast_existing_pipe),
    deal as
    (select distinct
      last_day(a.closedate,'quarter') as quarter,
      count(a.deal_id) over (partition by quarter) as dealcount
      from MIDAXO.DEV.DEAL a
      where a.pipeline_stage = 'closed won'
      and last_day(a.createdate,'quarter') = last_day(a.closedate,'quarter')
      and a.pipeline_type = 'direct')
SELECT c.ddate,
       c.forecast/(1-c.forecast)*e.forecast as plug,
       d.dealcount as actual,
       plug/d.dealcount - 1 as forecast_error
FROM cc c
left join ef e
  ON e.ddate = dateadd('day',5,last_day(dateadd('quarter',-1,c.ddate),'quarter'))
left join deal d
  on d.quarter = last_day(c.ddate, 'quarter')
where last_day(c.ddate,'quarter') <= last_day(current_date,'quarter')
order by c.ddate desc
) AS forecast_create_close),
    asp as 
    (select * from (WITH datespine AS
  (SELECT a.ddate,
          last_day(a.ddate,'quarter') AS quarter
   FROM midaxo.dev.datetable_clean a
   WHERE a.ddate between dateadd('quarter',-10,dateadd('day',1,last_day(current_date,'quarter'))) AND last_day(current_date,'quarter')),
     arr AS
  (SELECT a.closedate,
          a.recognized_arr
   FROM midaxo.dev.arr a
   WHERE a.pipeline_type = 'direct'),
     currentpipe AS
  (SELECT a.closedate,
          a.deal_amount
   FROM midaxo.dev.deal a
   WHERE a.pipeline_type = 'direct'
     AND a.pipeline_stage <> 'closed lost'
     AND a.deal_amount > 0)
SELECT distinct d.ddate,
                sum(a.recognized_arr) over (partition BY d.ddate) / count(a.recognized_arr) over (partition BY d.ddate) AS asp_forecast,
                sum(b.deal_amount) over (partition BY d.ddate) / count(b.deal_amount) over (partition BY d.ddate) AS asp_actual
FROM datespine d
left join arr a
  ON last_day(a.closedate,'quarter') between dateadd('quarter',-4,last_day(d.ddate,'quarter')) AND dateadd('quarter',-1,last_day(d.ddate,'quarter'))
left join currentpipe b
  ON last_day(b.closedate,'quarter') = last_day(d.ddate,'quarter')
WHERE d.ddate between dateadd('quarter',-5,dateadd('day',1,last_day(current_date,'quarter'))) AND last_day(current_date,'quarter')  
ORDER BY d.ddate ASC
) AS forecast_asp),
    deal as
    (select distinct last_day(a.closedate,'quarter') as qr,
      sum(a.recognized_arr) over (partition by qr) as arr
    from midaxo.dev.arr a
    where a.pipeline_type = 'direct'),
data AS
    (SELECT e.ddate,
        (e.forecast + c.plug) * a.asp_forecast as forecast,
        d.arr as actual,
        ((e.forecast + c.plug) * a.asp_forecast)/d.arr - 1 as forecast_error
FROM existingpipe e
left join createplug c
  ON e.ddate = c.ddate
left join 
asp a on
  a.ddate = e.ddate
left join deal d
  on last_day(e.ddate,'quarter') = d.qr
where date_part('day',e.ddate) = 5
  and last_day(e.ddate, 'quarter') = last_day(current_date,'quarter')),
target as 
(SELECT distinct 'Target' as target,
        last_day(b.ddate,'quarter') as ddate,
       sum(b.goal) over (partition by target) AS goal
FROM MIDAXO.DEV.KPI_TARGET b
where lower(b.type) = 'direct'
      and last_day(b.ddate, 'quarter') = last_day(current_date,'quarter'))
      
select f.*,
      t.goal,
      f.forecast/t.goal as attainment
from data f
left join target t
  on last_day(t.ddate,'quarter') = last_day(f.ddate,'quarter')