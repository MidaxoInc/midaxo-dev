--Table that shows daily deal value and stage from creation to close.
--need to pull in curent deal properties, unique historical amounts,
--historical deal stages, and historical close dates joined at
--the "day" level.
select
c.ddate,
c.week,
c.month,
c.quarter,
c.year,
c.yearweek,
c.yearmonth,
c.deal_id,
c.deal_name,
c.company_name,
e.pipeline_type,
e.pipeline_stage,
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

--join to add useable pipeline names
left join {{ref('PIPELINE_PROPERTY')}} e
  on d.deal_pipeline_stage_id = e.stage_id

where c.ddate <= d.closedate
order by ddate desc
