--select 1 row per deal per month, if it is the max date in month for that deal
select
  a.ddate,
  a.week,
  a.month,
  a.quarter,
  a.year,
  a.yearmonth,
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
from
-- this sets the row number from 1 to x by deal id within each month, and filters for the newest entry only
 (select
    r.*,
    row_number() over (
      partition by r.deal_id, r.yearmonth
      order by r.ddate desc
    ) x
  from {{ref('PIPELINE_DAILY')}} r
  ) a
where x = 1
