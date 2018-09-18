with
  engagement as (
  select
    x.id,
    y.contact_id as contact_id,
    x.created_at::timestamp_ntz as eventdate,
    lower(x.type) as event_type,
    'na' as event_action,
    x.owner_id::varchar as event_owner_campaign_url
  from RAW.HUBSPOT.ENGAGEMENT x

  left join RAW.HUBSPOT.ENGAGEMENT_CONTACT y
    on x.id = y.engagement_id
  ),
  contact as (
    select *
    from RAW.HUBSPOT.CONTACT
  ),
  mstd as (
    select *
    from {{ref('MSTD')}}
  )

select distinct
  md5(e.id) as event_id,
  e.id::varchar as engagement_id,
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
  lower(m.team) as event_source,
  e.event_owner_campaign_url::varchar as event_owner_campaign_url
from engagement e

left join contact c
  on e.contact_id = c.id
left join mstd m
  on m.owner_id = e.event_owner_campaign_url
  and last_day(m.ddate,'month') = last_day(e.eventdate,'month')
