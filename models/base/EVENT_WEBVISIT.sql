with visits AS
  (SELECT md5(h.company_id||h.timestamp) AS event_id,
          null AS contact_id,
          h.company_id,
          last_day(h.timestamp,'week') AS eventdate,
          'web_visit' AS event_type,
          h.value::timestamp_ntz AS lastvisit,
          max(h.value::timestamp_ntz) over (partition BY company_id
                                            ORDER BY h.timestamp ASC rows between unbounded preceding AND 1 preceding) AS max_lastvisit
   FROM RAW.HUBSPOT2.COMPANY_PROPERTY_HISTORY h
   WHERE h.name = 'hs_analytics_last_visit_timestamp')
SELECT  v.event_id,
        v.event_id as visit_id,
        v.contact_id,
        v.company_id,
        v.eventdate,
        v.event_type,
        'na' as event_action,
        'website' as event_source,
        'na' as event_owner_campaign_url
FROM visits v
WHERE v.lastvisit > v.max_lastvisit
  and h.value is not null
