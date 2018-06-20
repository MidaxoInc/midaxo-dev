--select 1 row per deal per week, if it is the earliest date in week for that deal (BOW pipeline)
select
  a.*
from (
  select
    r.*,
    row_number() over (
      partition by r.deal_id, r.yearweek
      order by r.ddate asc
    ) x
  from {{ref('PIPELINE_DAILY')}} r
) a
where x = 1
