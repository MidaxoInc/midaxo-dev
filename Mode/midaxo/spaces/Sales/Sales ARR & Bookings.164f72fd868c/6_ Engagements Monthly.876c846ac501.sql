with 
  arr as (
    select 
      a.year,
      a.yearmonth,
      a.monthlabel,
      a.pipeline_type,
      sum(a.recognized_arr) / '12' as net_bookings
    from midaxo.dev.arr a
    where a.pipeline_type = 'engagement'
    and last_day(a.closedate) <= last_day(current_date)
    group by a.year, a.yearmonth, a.monthlabel, a.pipeline_type
    order by a.yearmonth asc    
  ),
arr_target as (
  select
  b.ddate,
  b.year,
  b.yearmonth,
  b.monthlabel,
  lower(b.type) as type,
  b.metric,
  b.goal
  from midaxo.dev.kpi_target b
  where type = 'engagement'
  ),
mo_bookings as (
  select 
    arr_target.metric,
    arr_target.goal,
    arr_target.year,
    arr_target.yearmonth,
    arr.net_bookings,
    sum(arr.net_bookings) over(order by arr.yearmonth asc) as mo_bookings,
    mo_bookings/arr_target.goal as mo_attainment
  from arr
  full outer join arr_target
    on arr_target.yearmonth = arr.yearmonth
  order by yearmonth asc
  )

select
  mo_bookings.*,
  sum(mo_bookings.goal) over(order by mo_bookings.yearmonth asc) as target,
  case
    when mo_bookings.net_bookings is null then null
    else sum(mo_bookings.mo_bookings) over(order by mo_bookings.yearmonth asc) 
  end as bookings, 
  bookings/target as attainment
from mo_bookings
where mo_bookings.year = '2018'
order by mo_bookings.yearmonth