with
  event as (
    select * from {{ref('EVENT_CRM')}} crm
    where crm.event_type <> 'other'
    union
    select * from {{ref('EVENT_EMAIL')}} email
    where email.event_owner_campaign_url not in ('demo follow-up%','thank you for registering%')
    union
    select * from {{ref('EVENT_FORM')}} form
    union
    select * from {{ref('EVENT_DEAL')}} deal
    order by eventdate asc
  ),
  dealhistory as (
    select
      x.company_id,
      y.pipeline_type,
      y.pipeline_stage,
      y.pipeline_stageorder,
      x.validfrom,
      x.validto
    from {{ref('DEAL_ARCHIVE_CLEAN')}} x
      left join {{ref('PIPELINE_PROPERTY')}} y
        on
          x.deal_pipeline_id = y.pipeline_id
          and x.deal_pipeline_stage_id = y.stage_id
  ),
  datetable as (
    select * from {{ref('DATETABLE_CLEAN')}}
  )

select
  d.*,
  e.*,
  h.pipeline_type,
  h.pipeline_stage,
  h.pipeline_stageorder,
  row_number() over(
    partition by e.company_id
    order by e.company_id, e.eventdate asc
  ) as company_event_no,
  row_number() over(
    partition by e.contact_id
    order by e.contact_id, e.eventdate asc
  ) as contact_event_no
from event e

  left join datetable d
    on to_date(e.eventdate) = d.ddate

  left join dealhistory h
    on e.company_id = h.company_id
    and e.eventdate between h.validfrom and h.validto

order by e.company_id, e.eventdate asc
