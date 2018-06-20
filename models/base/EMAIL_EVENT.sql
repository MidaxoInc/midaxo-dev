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
  c.property_email as contact_email,
  e.created::timestamp_ntz as eventdate,
  'email' as eventcategory,
  e.type as eventtype,
  e.email_campaign_id as campaign_id
from email e

left join contact c
  on e.recipient = c.property_email

order by contact_email, eventdate asc
