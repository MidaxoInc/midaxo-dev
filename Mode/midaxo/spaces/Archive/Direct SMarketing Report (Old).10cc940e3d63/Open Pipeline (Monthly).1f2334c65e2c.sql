with 
  asp as (
    select
      d.ddate,
      avg(a.deal_amount) as t180_asp
    from MIDAXO.DEV.datetable_clean d
    left join MIDAXO.DEV.deal a
      on a.closedate between dateadd('day',-180, d.ddate) and d.ddate
    where 
      contains(a.pipeline_stage, 'won') = true
      and a.pipeline_type = 'direct'
    group by d.ddate
  ),
  company as (
  select
    c.id as company_id,
    c.property_country as country,
    c.property_is_icp as icp,
    c.property_icp_score as icp_score
  from raw.hubspot.company c
  ) 

select 
  a.ddate,
  a.year,
  a.yearmonth,
  a.monthlabel,
  case when c.icp_score is null then 'non-icp'
  else c.icp_score
  end as icp_score,
  a.deal_name,
  a.pipeline_type,
  a.pipeline_stageorder,
  a.pipeline_stage,
  a.pipeline_stageorder || ' - ' || a.pipeline_stage as stage,
  case
    when (a.pipeline_stage in ('qualification') or a.deal_amount=0) then asp.t180_asp
    else a.deal_amount
    end as deal_amount
from MIDAXO.DEV.PIPELINE_daily a
left join asp 
  on asp.ddate = a.ddate
left join company c
   on a.company_id = c.company_id
where last_day(a.ddate,'month') > dateadd('month',-15,last_day(current_date,'month'))
  and pipeline_type = 'direct'
  and a.pipeline_stage not in ('closed won','closed lost','activation','nurture')
  and extract('day', a.ddate) = '5'
order by a.ddate, a.pipeline_stageorder desc