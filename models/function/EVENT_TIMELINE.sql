with
  events as (
    select * from {{ref('EVENT_CRM')}}
    union
    select * from {{ref('EVENT_EMAIL')}}
    union
    select * from {{ref('EVENT_FORM')}}
    order by eventdate asc
  )

select
  e.*,
  row_number() over(
    partition by company_id
    order by company_id, eventdate asc
  ) as companytime,

  row_number() over(
    partition by contact_id
    order by contact_id, eventdate asc
  ) as contacttime
from events e
order by company_id, eventdate asc
