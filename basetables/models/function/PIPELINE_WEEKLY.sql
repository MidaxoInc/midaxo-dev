--select 1 row per deal per week, if it is the max date in week for that deal
select
  a.ddate,
  a.week,
  a.quarter,
  a.year,
  concat(a.year,a.week) as yearweek,
  a.deal_id,
  a.deal_pipeline_id,
  a.deal_pipeline_stage_id,
  a.deal_amount,
  a.createdate,
  a.closedate,
  a.validfrom,
  a.validto
from {{ref('PIPELINE_DAILY')}} a
inner join (
  select
  b.deal_id,
  max(b.ddate) as maxdate
  from {{ref('PIPELINE_DAILY')}} b
  group by b.deal_id, b.year, b.week
) c
  on c.deal_id = a.deal_id
    and c.maxdate = a.ddate
order by yearweek desc
