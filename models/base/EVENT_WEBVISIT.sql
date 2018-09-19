SELECT md5(h.company_id||h.timestamp) AS event_id,
    null AS contact_id,
    h.company_id,
    h.timestamp::timestamp_ntz AS eventdate,
    'web_visit' AS event_type,
    'na' as event_source
FROM RAW.HUBSPOT2.COMPANY_PROPERTY_HISTORY h
WHERE h.name = 'hs_analytics_num_visits'
