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

order by e.company_id, e.eventdate asc
