with
  engagement as (
  select
    y.contact_id as contact_id,
    x.timestamp::timestamp_ntz as eventdate,
    lower(x.type) as event_type,
    'na' as event_action,
    'sales' as event_source,
    x.owner_id::varchar as event_owner_campaign_url
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
    when contains(e.event_type,'email') then 'sales_email'
    when contains(e.event_type,'call') then 'sales_call'
    when contains(e.event_type,'meeting') then 'meeting'
    else 'other'
  end as event_type,
  e.event_action,
  e.event_source::varchar as event_source,
  e.event_owner_campaign_url::varchar as event_owner_campaign_url
from engagement e

left join contact c
  on e.contact_id = c.id
