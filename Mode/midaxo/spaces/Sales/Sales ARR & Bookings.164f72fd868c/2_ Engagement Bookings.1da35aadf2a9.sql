with 
  arr as (
    select 
      a.year,
      a.yearmonth,
      a.monthlabel,
      a.pipeline_type,
      ifnull(sum(a.recognized_arr) / 12,0) as net_bookings
    from midaxo.dev.arr a
    where a.pipeline_type = 'engagement'
    and last_day(a.closedate) <= last_day(current_date)
    group by a.year, a.yearmonth, a.monthlabel, a.pipeline_type
    order by a.yearmonth asc    
  ),
arr_target as (
  select
  b.yearmonth,
  b.monthlabel,
  lower(b.type) as type,
  b.metric,
  b.goal
  from midaxo.dev.kpi_target b
  ),
mo_bookings as (
  select 
    arr_target.metric,
    arr_target.goal,
    arr.*,
    sum(arr.net_bookings) over(order by arr.yearmonth asc) as mo_bookings,
    mo_bookings/arr_target.goal as mo_attainment
  from arr
  left join arr_target 
    on arr_target.yearmonth = arr.yearmonth
      and arr.pipeline_type = arr_target.type
  order by yearmonth asc
  )

select
  mo_bookings.*,
  sum(mo_bookings.goal) over(order by mo_bookings.yearmonth asc) as cum_goal,
  sum(mo_bookings.mo_bookings) over(order by mo_bookings.yearmonth asc) as cum_bookings, 
  cum_bookings/cum_goal as cum_attainment
from mo_bookings
where mo_bookings.year = '2018'