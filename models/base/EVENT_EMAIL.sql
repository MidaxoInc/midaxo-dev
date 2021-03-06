  -- Only pulls emails that were engaged by the recipient, and not "form follow ups" i.e. demo scheduling
with
  email as (
    select
      e.*,
      c.name,
      row_number() over (partition by e.sent_by_id, e.recipient order by e.created) as dupes
    from raw.hubspot2.EMAIL_EVENT e
    left join raw.hubspot2.email_campaign c
      on e.email_campaign_id = c.id
    where e.type in ('OPEN','CLICK','PRINT','FORWARD')
      and created is not null
    ),
  contact as (
    select *
    from raw.hubspot2.CONTACT
    )

select
  md5(e.id) as event_id,
  e.id::varchar as email_id,
  c.id as contact_id,
  c.property_associatedcompanyid as company_id,
  e.created::timestamp_ntz as eventdate,
  'marketing_email' as event_type,
  case
    when e.type = 'OPEN' then 'open'
    else 'click'
    end as event_action,
  'email' as event_source,
  lower(e.name)::varchar as event_owner_campaign_url
from email e

left join contact c
  on e.recipient = c.property_email
where e.dupes = 1
order by contact_id, eventdate asc
