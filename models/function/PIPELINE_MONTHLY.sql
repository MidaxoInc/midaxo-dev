--select 1 row per deal per month, if it is the earliest date in month for that deal (BOM Pipeline Value)
select
  a.*
from
-- this sets the row number from 1 to x by deal id within each month, and filters for the newest entry only
 (select
    r.*,
    row_number() over (
      partition by r.deal_id, r.yearmonth
      order by r.ddate asc
    ) x
  from {{ref('PIPELINE_DAILY')}} r
  ) a
where x = 1
