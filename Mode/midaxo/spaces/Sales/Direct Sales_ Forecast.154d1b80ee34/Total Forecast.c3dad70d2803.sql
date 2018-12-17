with existingpipe AS
  (SELECT *
   FROM {{ @forecast_existing_pipe }}),
     createplug AS
  (SELECT *
   FROM {{ @forecast_create_close }}),
     asp AS
  (SELECT *
   FROM {{ @forecast_asp }}),
     deal AS
  (SELECT distinct last_day(a.closedate,'quarter') AS qr,
                   sum(a.recognized_arr) over (partition BY qr) AS arr
   FROM midaxo.dev.arr a
   WHERE a.pipeline_type = 'direct')
SELECT e.ddate,
       (e.forecast + c.plug) * a.asp_forecast AS forecast,
       d.arr AS actual,
       CASE
           WHEN last_day(e.ddate,'quarter') <> last_day(current_date,'quarter') 
            THEN ((e.forecast + c.plug) * a.asp_forecast)/d.arr - 1
           ELSE NULL
       END AS forecast_error
FROM existingpipe e
left join createplug c
  ON e.ddate = c.ddate
left join asp a
  ON a.ddate = e.ddate
left join deal d
  ON last_day(e.ddate,'quarter') = d.qr
WHERE date_part('day',e.ddate) = iff(last_day(e.ddate,'month') = last_day(current_date,'month')
                                     AND date_part('day',dateadd('day',-1,current_date))<5,date_part('day',dateadd('day',-1,current_date)),5)
  AND e.ddate <= current_date
ORDER BY e.ddate ASC