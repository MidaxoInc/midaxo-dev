--Table that shows daily deal value and stage from creation to close or current date.
with
  datespine as (select * from {{ref('DATETABLE_CLEAN')}}),
  dealtable as (select * from {{ref('DEAL')}}),
  dealhistory as (select * from {{ref('DEAL_ARCHIVE_CLEAN')}}),
  pipelines as (select * from {{ref('PIPELINE_PROPERTY')}})

select
datespine.*,
dealtable.deal_id,
dealtable.deal_name,
dealtable.company_name,
pipelines.pipeline_type,
pipelines.pipeline_stage,
dealhistory.deal_amount,
dealhistory.createdate,
dealhistory.closedate,
dealhistory.validfrom,
dealhistory.validto
from datespine

inner join dealtable
  on (
    dealtable.createdate <= datespine.ddate
    and least(current_date(), dealtable.closedate) >= datespine.ddate
  )

left join dealhistory
  on (
    dealhistory.deal_id = dealtable.deal_id
    and dealhistory.validfrom <= datespine.ddate
    and dealhistory.validto >= datespine.ddate
  )

left join pipelines
  on dealhistory.deal_pipeline_stage_id = pipelines.stage_id

where datespine.ddate <= dealtable.closedate
order by ddate desc
