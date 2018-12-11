WITH datespine AS
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
SELECT distinct last_day(d.ddate,'quarter') as quarter,
                sum(a.recognized_arr) over (partition BY d.ddate) / count(a.recognized_arr) over (partition BY d.ddate) AS forecast,
                sum(b.deal_amount) over (partition BY d.ddate) / count(b.deal_amount) over (partition BY d.ddate) AS actual,
                forecast/actual - 1 as forecast_error
FROM datespine d
left join arr a
  ON last_day(a.closedate,'quarter') between dateadd('quarter',-4,last_day(d.ddate,'quarter')) AND dateadd('quarter',-1,last_day(d.ddate,'quarter'))
left join currentpipe b
  ON last_day(b.closedate,'quarter') = last_day(d.ddate,'quarter')
WHERE d.ddate between dateadd('quarter',-5,dateadd('day',1,last_day(current_date,'quarter'))) AND last_day(current_date,'quarter')  
ORDER BY quarter ASC