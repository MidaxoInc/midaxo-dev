with
  engagement as (
  select
    y.contact_id as contact_id,
    x.timestamp::timestamp_ntz as eventdate,
    lower(x.type) as event_type,
    null as event_action,
    x.owner_id as event_source
  from RAW.HUBSPOT.ENGAGEMENT x

  left join RAW.HUBSPOT.ENGAGEMENT_CONTACT y
    on x.id = y.engagement_id
  ),
  contact as (
    select *
    from RAW.HUBSPOT.CONTACT
  )

select
  c.id as contact_id,
  c.property_associatedcompanyid as company_id,
  e.eventdate,
  case
    when contains(e.event_type,'email') then 'email'
    when contains(e.event_type,'call') then 'call'
    when contains(e.event_type,'meeting') then 'meeting'
    else 'other'
  end as event_type,
  e.event_action,
  e.event_source
from engagement e

left join contact c
  on e.contact_id = c.id
