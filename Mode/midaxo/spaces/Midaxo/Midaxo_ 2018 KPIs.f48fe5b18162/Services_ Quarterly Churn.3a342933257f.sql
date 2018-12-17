with arr_mo AS
  (SELECT distinct last_day(a.closedate,'quarter') AS ddate,
                   sum(a.recognized_arr) over (partition BY last_day(a.closedate,'quarter')) AS recognized_arr
   FROM midaxo.dev.arr a
   WHERE last_day(a.closedate) <= last_day(current_date)
    and a.pipeline_type = 'renewal'
    and a.pipeline_stage = 'churned'),
     arr_target AS
  (SELECT distinct last_day(b.ddate,'quarter') AS ddate,
                   lower(b.type) AS pipeline_type,
                   b.metric,
                   sum(b.goal) over (partition BY lower(b.type), last_day(b.ddate,'quarter')) AS goal
   FROM midaxo.dev.kpi_target b
   where lower(b.type) = 'churn')
SELECT distinct arr_target.ddate,
                arr_target.pipeline_type,
                arr_target.goal AS "Target",
                arr_mo.recognized_arr AS "ARR",
                "ARR"/"Target" AS "Attainment"
FROM arr_target
left join arr_mo
  ON arr_target.ddate = arr_mo.ddate
ORDER BY arr_target.ddate