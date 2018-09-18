--CTE--
with raw as
(  SELECT
      a.conversation_id::varchar AS drift_conversation_id,
       a.contact_id::varchar AS drift_contact_id,
       a.contact_email::string AS contact_email,
       a.owner_email::string AS owner_email,
       convert_timezone('America/New_York','UTC',to_timestamp_ntz(a.convo_starttime,'mm/dd/yyyy hh24:mi')) AS startdate,
       date_part('min',to_time(a.convo_responsetime_m_s_ms_,'mi:ss:ff3')) + date_part('sec',to_time(a.convo_responsetime_m_s_ms_,'mi:ss:ff3'))/60 AS responsemins,
       convert_timezone('America/New_York','UTC',to_timestamp_ntz(a.convo_closetime,'mm/dd/yyyy hh24:mi')) AS closedate
FROM raw.manual.drift a)

--chat start--
select
  md5(r.drift_conversation_id||r.startdate) as event_id,
  r.drift_conversation_id::varchar AS conversation_id,
  c.id AS contact_id,
  c.PROPERTY_ASSOCIATEDCOMPANYID AS company_id,
  ifnull(f.eventdate,r.startdate) AS eventdate,
  ifnull(f.event_type,'chat_other') as event_type,
  ifnull(f.event_action,'na') as event_action,
  ifnull(f.event_source,'other') as event_source,
  m.owner_id::varchar as event_owner_campaign_url
FROM raw r
left join raw.hubspot.contact c
  ON c.property_email = r.contact_email
left join {{ref('EVENT_FORM')}} f
  on f.contact_id = c.id
  and to_date(f.eventdate) = to_date(r.startdate)
  and f.event_type = 'chat conversion'
left join {{ref('MSTD')}} m
  on m.email = r.owner_email
  and m.ddate = last_day(ifnull(f.eventdate,r.startdate),'month')
union all
--chat response--
select
  md5(r.drift_conversation_id||r.startdate||r.responsemins) as event_id,
  r.drift_conversation_id::varchar AS conversation_id,
  c.id AS contact_id,
  c.PROPERTY_ASSOCIATEDCOMPANYID AS company_id,
  dateadd('sec',r.responsemins,ifnull(f.eventdate,r.startdate)) AS eventdate,
  'chat_response' as event_type,
  ifnull(f.event_action,'na') as event_action,
  'drift' as event_source,
  m.owner_id::varchar as event_owner_campaign_url
FROM raw r
left join raw.hubspot.contact c
  ON c.property_email = r.contact_email
left join {{ref('EVENT_FORM')}} f
  on f.contact_id = c.id
  and to_date(f.eventdate) = to_date(r.startdate)
left join {{ref('MSTD')}} m
  on m.email = r.owner_email
  and m.ddate = last_day(ifnull(f.eventdate,r.startdate),'month')
