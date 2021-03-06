with
  event as (
    select
      a.*
    from {{ref('EVENT_TIMELINE')}} a
    ),
  pipecreated as (
    select
      to_date(b.validfrom) as ddate,
      b.company_id,
      b.deal_id,
      b.deal_name,
      b.pipeline_type,
      b.pipeline_stage,
      row_number() over (partition by b.deal_id order by b.validfrom asc) as x
    from {{ref('DEAL_ARCHIVE_CLEAN')}}  b
    where
      b.pipeline_type = 'direct'
      and b.pipeline_stage not in ('activation','nurture','closed lost')
  ),

  dealvalue as
  (select a.*
  from {{ref('DEAL')}} a
  where a.pipeline_type = 'direct'
    and a.pipeline_stage not in ('activation','nurture','closed lost')),

  attribution as (
   select
     e.*,
     p.ddate as dealcreatedate,
     min(e.eventdate) over (partition by p.deal_id) as firsttouchdate,
     p.deal_id,
     p.deal_name
   from event e
   inner join pipecreated p
    on
      p.company_id = e.company_id
      and p.x=1
      and e.eventdate <= p.ddate
    where e.event_category <> 'other'
  ),

  asp as (
    select distinct
      d.ddate,
      avg(a.deal_amount) over (partition by d.ddate) as t180_asp
    from {{ref('DATETABLE_CLEAN')}} d
    left join {{ref('DEAL')}} a
      on a.closedate between dateadd('month',-7, dateadd('day',1,last_day(d.ddate,'month'))) and last_day(d.ddate,'month')
    where
      contains(a.pipeline_stage, 'won') = true
      and a.pipeline_type = 'direct'
  ),

  company as (
    select
      c.id as company_id,
      c.property_name as company_name,
      c.property_country as country,
      c.property_sales_territory as territory,
      c.property_icp_score as icp_score
    from raw.hubspot2.company c
  )

select distinct
  t.*,
  c.company_name,
  c.territory,
  c.country,
  c.icp_score,
  CASE
  WHEN ifnull(d.recognized_arr,0) > 0 then d.recognized_arr
  WHEN ifnull(d.deal_amount,0) > 0 then d.deal_amount
  ELSE a.t180_asp END AS deal_value,
  count(*) over (partition by t.deal_id) as total_eventcount,
  count(*) over (partition by t.deal_id, t.event_id, t.contact_id) as detail_eventcount,
  count(*) over (partition by t.deal_id, t.event_id, t.contact_id)/count(*) over (partition by t.deal_id) as detail_share
from attribution t
left join asp a
  on a.ddate = t.dealcreatedate
left join company c
  on c.company_id = t.company_id
left join dealvalue d
  on d.deal_id = t.deal_id
