WITH opendeal AS
  (SELECT *
   FROM {{ @forecast_open_deals }}),
     winrate AS
  (SELECT *
   FROM {{ @forecast_real_win_rate }}),
     wondeal AS
  (SELECT *
   FROM {{ @forecast_won_deals }}
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
-- WHERE f.ddate <= current_date
--  AND date_part('day',f.ddate) = 5
ORDER BY f.ddate DESC