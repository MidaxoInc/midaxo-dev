--Table that shows daily deal value and stage from creation to close.
--need to pull in curent deal properties, unique historical amounts,
--historical deal stages, and historical close dates joined at
--the "day" level.
select
c.ddate,
c.week,
c.quarter,
c.year,
c.deal_id,
d.deal_pipeline_id,
d.deal_pipeline_stage_id,
d.deal_amount,
d.createdate,
d.closedate,
d.validfrom,
d.validto
from (
-- subquery for joined datespine and deal info
  select *
  from {{ref('DATETABLE_CLEAN')}} a
  --join deal ids to all dates between create and close date
  inner join {{ref('DEAL')}} b
    on (
      b.createdate <= a.ddate
      and b.closedate >= a.ddate
    )
  ) c
--join deal history table to date and deal info table
left join {{ref('DEAL_ARCHIVE_CLEAN')}} d
  on (
    d.deal_id = c.deal_id
    and d.validfrom <= c.ddate
    and d.validto >= c.ddate
  )
-- select max entry per day and deal by most recent changedate
--left join {{ref('DEAL_ARCHIVE_CLEAN')}} e
--  on (
--    e.deal_id = c.deal_id
--    and e.validfrom <= c.ddate
--    and e.validto >= c.ddate
--    and e.validfrom > d.validfrom
--  )
-- where e.validfrom is null
order by ddate desc
