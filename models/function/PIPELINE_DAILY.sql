--Table that shows daily deal value and stage from creation to close or current date.
with
  datespine as (select * from {{ref('DATETABLE_CLEAN')}}),
  dealtable as (select * from {{ref('DEAL')}}),
  dealhistory as (select * from {{ref('DEAL_ARCHIVE_CLEAN')}}),
  pipelines as (select * from {{ref('PIPELINE_PROPERTY')}}),
  asp as (
    select
      d.ddate,
      avg(a.deal_amount) as t180_asp
    from datespine d
    left join dealtable a
      on a.closedate between dateadd('day',-180, d.ddate) and d.ddate
    where
      contains(a.pipeline_stage, 'won') = true
      and a.pipeline_type = 'direct'
    group by d.ddate
  )

select
  d.*,
  t.deal_id,
  t.company_id,
  t.deal_name,
  t.company_name,
  p.pipeline_type,
  p.pipeline_stage,
  p.pipeline_stageorder,
  case
    when (p.pipeline_stage in ('qualification') or h.deal_amount = 0) then a.t180_asp
    else h.deal_amount
  end as deal_amount,
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

  left join asp a
    on to_date(a.ddate) = to_date(d.ddate)

where d.ddate <= t.closedate
order by ddate desc
