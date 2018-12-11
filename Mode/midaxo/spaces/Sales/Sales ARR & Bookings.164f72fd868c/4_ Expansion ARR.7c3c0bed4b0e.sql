with arr_mo AS
  (SELECT a.year,
          a.yearmonth,
          a.monthlabel,
          a.pipeline_type,
          sum(a.recognized_arr) AS recognized_arr
   FROM midaxo.dev.arr a
   WHERE 
   --a.finance_verified = true
     a.pipeline_type = 'expansion'
     AND last_day(a.closedate) <= last_day(current_date)
   GROUP BY a.year,
            a.yearmonth,
            a.monthlabel,
            a.pipeline_type
   ORDER BY a.yearmonth ASC ),
     arr_target AS
  (SELECT b.year,
          b.yearmonth,
          b.ddate,
          b.monthlabel,
          lower(b.type) AS TYPE,
          b.metric,
          b.goal
   FROM midaxo.dev.kpi_target b
   WHERE b.type = 'expansion' )
SELECT arr_target.metric,
       arr_target.goal,
       arr_target.year,
       arr_target.yearmonth,
       arr_target.type,
       arr_mo.recognized_arr,
       arr_mo.recognized_arr/arr_target.goal AS mo_attainment,
       sum(arr_target.goal) over(
                                 ORDER BY arr_target.yearmonth) AS cum_goal,
       CASE
           WHEN last_day(arr_target.ddate,'month') > last_day(current_date(),'month') then null
           ELSE sum(arr_mo.recognized_arr) over(
                                                ORDER BY arr_target.yearmonth)
       END AS cum_arr,
       cum_arr/cum_goal AS cum_attainment
FROM arr_target
left join arr_mo
  ON arr_target.yearmonth = arr_mo.yearmonth
ORDER BY arr_target.yearmonth