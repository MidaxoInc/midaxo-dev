with 
arr_mo as
  (select 
    a.year,
    a.yearmonth,
    a.monthlabel,
    a.pipeline_type,
    a.deal_type,
    sum(a.recognized_arr) as recognized_arr
  from midaxo.dev.arr a
  where 
    --a.finance_verified = true
     a.pipeline_type = 'partner new'
    and last_day(a.closedate) <= last_day(current_date)
  group by a.year, a.yearmonth, a.monthlabel, a.pipeline_type, a.deal_type
  order by a.yearmonth asc
  ),
arr_target as
  (select
  b.year,
  b.yearmonth,
  b.ddate,
  b.monthlabel,
  lower(b.type) as type,
  b.metric,
  b.goal
  from midaxo.dev.kpi_target b
  where b.type = 'partner new'
  )

select 
  arr_target.metric,
  arr_target.goal,
  arr_target.year,
  arr_target.yearmonth,
  arr_target.type,
  arr_mo.recognized_arr,
  arr_mo.recognized_arr/arr_target.goal as mo_attainment,
  sum(arr_target.goal) over(order by arr_target.yearmonth) as cum_goal,
  case 
    when last_day(arr_target.ddate,'month') > last_day(current_date(),'month') then null 
    else sum(arr_mo.recognized_arr) over(order by arr_target.yearmonth) 
  end as cum_arr,
  cum_arr/cum_goal as cum_attainment
from arr_target
left join arr_mo 
  on arr_target.yearmonth = arr_mo.yearmonth
