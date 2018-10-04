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
    union
    select * from {{ref('EVENT_WEBVISIT')}} web
    union
    select * from {{ref('EVENT_DEAL')}} deal
  ),
  datetable as (
    select * from {{ref('DATETABLE_CLEAN')}}
  )

select
  d.*,
  e.*,
  case
    when e.event_type in ('form_conversion','chat_conversion', 'web_visit') then 'marketing'
    when e.event_owner_campaign_url ilike '%demo follow-up%' then 'marketing'
    when e.event_owner_campaign_url ilike '%webinar%' then 'marketing'
    when e.event_source ilike 'sdr' then 'sdr'
    when e.event_type in ('chat_response') then 'sdr'
    when e.event_type in ('sales_email','sales_call','meeting') then 'ae'
    else 'other'
  end as event_category,
  row_number() over(
    partition by e.company_id
    order by e.company_id, e.eventdate asc
  ) as company_event_no
from event e

  left join datetable d
    on to_date(e.eventdate) = d.ddate

where to_date(e.eventdate) <= current_date

order by e.company_id, e.eventdate desc
