with
  form as (
  select
    x.contact_id,
    x.timestamp::timestamp_ntz as eventdate,
    lower(x.title) as form_label,
    lower(x.page_url) as form_url,
    y.lead_nurturing_campaign_id as campaign_id
  from RAW.HUBSPOT.CONTACT_FORM_SUBMISSION x

  left join raw.hubspot.form y
    on x.form_id = y.guid
  ),
  contact as (
    select *
    from RAW.HUBSPOT.CONTACT
    )

select
  c.id as contact_id,
  c.property_associatedcompanyid as company_id,
  f.eventdate,
  case
    when contains(f.form_label,'drift') then 'chat'
    else 'form'
  end as event_type,
  case
    when contains(f.form_label,'webinar') then 'webinar'
    when contains(f.form_label,'demo') then 'demo'
    when contains(f.form_url, 'demo') then 'demo'
    when contains(f.form_url, 'webinar') then 'webinar'
    else 'other'
  end as event_action,
  case
    when contains(f.form_url, 'source=hs_email') then 'email'
    when contains(f.form_url, 'source=email') then 'email'
    when contains(f.form_url, 'source=hs_automation') then 'email'
    when contains(f.form_url, 'source=linkedin') then 'linkedin'
    when contains(f.form_url, 'source=twitter') then 'twitter'
    when contains(f.form_url, 'source=facebook') then 'facebook'
    when contains(f.form_url, 'source=adwords') then 'adwords'
    when contains(f.form_url, 'source=ppc') then 'ppc'
    else 'direct'
  end as event_source,
  f.campaign_id
from form f

left join contact c
  on f.contact_id = c.id

order by contact_id, f.eventdate asc