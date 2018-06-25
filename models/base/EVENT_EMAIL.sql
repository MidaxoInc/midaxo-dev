  -- Only pulls emails that were engaged by the recipient
with
  email as (
    select *
    from RAW.HUBSPOT.EMAIL_EVENT
    where type in ('OPEN','CLICK','PRINT','FORWARD')
      and created is not null
    ),
  contact as (
    select *
    from RAW.HUBSPOT.CONTACT
    )

select
  c.id as contact_id,
  c.property_associatedcompanyid as company_id,
  e.created::timestamp_ntz as eventdate,
  'marketing_email' as event_type,
  case
    when e.type = 'OPEN' then 'open'
    else 'click'
    end as event_action,
  e.email_campaign_id::varchar as event_source
from email e

left join contact c
  on e.recipient = c.property_email

order by contact_id, eventdate asc
