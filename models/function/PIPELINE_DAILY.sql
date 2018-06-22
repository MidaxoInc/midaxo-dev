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
h.deal_amount,
h.createdate,
h.closedate,
h.validfrom,
h.validto
from datespine d

inner join dealtable t
  on (
    dealtable.createdate <= datespine.ddate
    and least(current_date(), dealtable.closedate) >= datespine.ddate
  )

left join dealhistory h
  on (
    dealhistory.deal_id = dealtable.deal_id
    and dealhistory.validfrom <= datespine.ddate
    and dealhistory.validto >= datespine.ddate
  )

left join pipelines p
  on dealhistory.deal_pipeline_stage_id = pipelines.stage_id

where datespine.ddate <= dealtable.closedate
order by ddate desc
