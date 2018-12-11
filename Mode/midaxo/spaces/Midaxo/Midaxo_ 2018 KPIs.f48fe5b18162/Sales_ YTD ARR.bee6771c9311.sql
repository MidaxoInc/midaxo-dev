with arr_mo AS
  (SELECT last_day(a.closedate,'month') AS ddate,
          sum(a.recognized_arr) over (order by ddate asc) as arr
   FROM midaxo.dev.arr a
   WHERE pipeline_type in ('direct',
                           'expansion',
                           'partner new')
     AND last_day(a.closedate,'month') <= last_day(dateadd('month',-1,current_date),'month')
     AND date_part('year',a.closedate) = date_part('year',last_day(dateadd('month',-1,current_date),'month'))),
     arr_target AS
  (SELECT last_day(b.ddate,'month') AS ddate,
          sum(b.goal) over (order by ddate asc) as goal
   FROM midaxo.dev.kpi_target b
   WHERE lower(b.type) in ('direct',
                           'expansion',
                           'partner new')
     AND last_day(b.ddate,'month') < last_day(current_date,'month'))

SELECT distinct arr_mo.ddate,
                max(arr_mo.arr) over (partition BY arr_mo.ddate) AS arr,
                max(arr_target.goal) over (partition BY arr_mo.ddate) AS goal,
                max(arr_mo.arr) over (partition BY arr_mo.ddate) / max(arr_target.goal) over (partition BY arr_mo.ddate) as attainment
FROM arr_target
left join arr_mo
  on arr_mo.ddate = arr_target.ddate
order by ddate asc

