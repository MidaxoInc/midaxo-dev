with 
arr_mo as
  (select 
    a.year,
    a.yearmonth,
    a.monthlabel,
    sum(a.recognized_arr) as recognized_arr
  from midaxo.dev.arr a
  where 
    --a.finance_verified = true
     pipeline_type in ('direct','expansion','partner new')  
    and last_day(a.closedate) <= last_day(current_date)

  group by a.year, a.yearmonth, a.monthlabel
  order by a.yearmonth asc
  ),
arr_target as
  (select
  b.yearmonth,
  b.monthlabel,
  b.metric,
  sum(b.goal) as goal
  from midaxo.dev.kpi_target b
  where lower(b.type) in ('direct','expansion','partner new')  
  group by b.yearmonth, b.monthlabel, b.metric
  )

select 
  arr_target.metric,
  arr_target.goal,
  arr_mo.year,
  arr_mo.yearmonth,
  arr_mo.recognized_arr,
  arr_mo.recognized_arr/arr_target.goal as mo_attainment,
  sum(arr_target.goal) over(order by arr_mo.yearmonth asc) as cum_goal,
  sum(arr_mo.recognized_arr) over(order by arr_mo.yearmonth asc) as cum_arr,
  cum_arr/cum_goal as cum_attainment
from arr_mo
left join arr_target 
  on arr_target.yearmonth = arr_mo.yearmonth
where 
  arr_mo.year = '2018'
ORDER BY arr_mo.yearmonth asc
