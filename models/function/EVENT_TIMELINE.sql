with
  event as (
    select * from {{ref('EVENT_CRM')}} crm
    where crm.event_type <> 'other'
    union
    select * from {{ref('EVENT_EMAIL')}} email
    where email.event_owner_campaign_url not in ('demo follow-up%','thank you for registering%') --remove transactional emails sent as a conversion follow-up
    union
    select * from {{ref('EVENT_FORM')}} form
    where form.event_type <> 'chat conversion' -- pulling chat conversion info into event_drift
    union
    select * from {{ref('EVENT_DRIFT')}} drift
  ),
  datetable as (
    select * from {{ref('DATETABLE_CLEAN')}}
  )

select
  d.*,
  e.*,
  row_number() over(
    partition by e.company_id
    order by e.company_id, e.eventdate asc
  ) as company_event_no
from event e

  left join datetable d
    on to_date(e.eventdate) = d.ddate

order by e.company_id, e.eventdate desc
