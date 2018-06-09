--select 1 row per deal per week, if it is the max date in week for that deal
select
  a.ddate,
  a.week,
  a.month,
  a.quarter,
  a.year,
  a.yearweek,
  a.deal_id,
  a.deal_name,
  a.company_name,
  a.pipeline_type,
  a.pipeline_stage,
  a.deal_amount,
  a.createdate,
  a.closedate,
  a.validfrom,
  a.validto
from (
  select
    r.*,
    row_number() over (
      partition by r.deal_id, r.yearweek
      order by r.ddate desc
    ) x
  from {{ref('PIPELINE_DAILY')}} r
) a
where x = 1
