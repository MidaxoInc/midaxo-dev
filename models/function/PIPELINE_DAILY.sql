--Table that shows daily deal value and stage from creation to close or current date.
with
  datespine as (select * from {{ref('DATETABLE_CLEAN')}}),
  dealtable as (select * from {{ref('DEAL')}}),
  dealhistory as (select * from {{ref('DEAL_ARCHIVE_CLEAN')}}),
  pipelines as (select * from {{ref('PIPELINE_PROPERTY')}})

select
d.*,
t.deal_id,
t.deal_name,
t.company_name,
p.pipeline_type,
p.pipeline_stage,
p.pipeline_stageorder,
h.deal_amount,
h.createdate,
h.closedate,
h.validfrom,
h.validto
from datespine d

inner join dealtable t
  on (
    t.createdate <= d.ddate
    and least(current_date(), t.closedate) >= d.ddate
  )

left join dealhistory h
  on (
    h.deal_id = t.deal_id
    and h.validfrom <= d.ddate
    and h.validto >= d.ddate
  )

left join pipelines p
  on h.deal_pipeline_stage_id = p.stage_id

where d.ddate <= t.closedate
order by ddate desc
