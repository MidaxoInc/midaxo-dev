with arr_mo AS
  (SELECT distinct last_day(a.closedate,'quarter') AS ddate,
                   a.pipeline_type,
                   sum(a.recognized_arr) over (partition BY last_day(a.closedate,'quarter'), a.pipeline_type) AS recognized_arr
   FROM midaxo.dev.arr a
   WHERE last_day(a.closedate) <= last_day(current_date) ),
     arr_target AS
  (SELECT distinct last_day(b.ddate,'quarter') AS ddate,
                   lower(b.type) AS pipeline_type,
                   b.metric,
                   sum(b.goal) over (partition BY lower(b.type), last_day(b.ddate,'quarter')) AS goal
   FROM midaxo.dev.kpi_target b)
SELECT distinct arr_target.ddate,
                arr_target.pipeline_type,
                arr_target.goal AS "Target",
                arr_mo.recognized_arr AS "ARR",
                "ARR"/"Target" AS "Attainment"
FROM arr_target
left join arr_mo
  ON arr_target.ddate = arr_mo.ddate
  AND arr_target.pipeline_type = arr_mo.pipeline_type
ORDER BY arr_target.ddate