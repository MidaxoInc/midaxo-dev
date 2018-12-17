with event AS
  (SELECT a.*,
          row_number() over (partition BY a.contact_id
                             ORDER BY a.eventdate ASC) AS contact_event_no
   FROM midaxo.dev.event_timeline a)
SELECT distinct to_date(e.eventdate) AS ddate,
                e.company_id,
                e.contact_id,
                t.property_firstname || ' ' || t.property_lastname AS contact_name,
                c.property_name AS company_name,
                c.property_sales_territory AS territory,
                ifnull(c.property_icp_score,'Non-ICP') AS icp_score,
                e.event_category,
                e.event_type,
                e.event_source,
                e.event_owner_campaign_url,
                case  when c.property_sales_territory = 'nam' then 'nam'
                      when c.property_sales_territory in ('uk','nordics','benelux','dach','eur') then 'emea'
                      when c.property_sales_territory is null then 'enrichment'
                      else 'un-workable' end as geo,
                count(e.event_id) over (partition BY ddate,e.contact_id, e.event_type, e.event_source) AS leads
FROM event e
left join raw.hubspot2.company c
  ON c.id = e.company_id
left join raw.hubspot2.contact t
  ON t.id = e.contact_id
WHERE last_day(e.eventdate,'month') between to_date('2017-01-01') AND last_day(current_date,'month')
  AND (e.contact_event_no = 1
       OR e.event_type in ('form_conversion',
                           'chat_conversion'))
  AND e.contact_event_no = 1
  AND ifnull(t.property_bad_fit_lead,'false') = 'false'
  AND ifnull(c.property_partner,'false') = 'false'
  AND ifnull(c.property_partner_engagement,'false') = 'false'
  AND e.event_type in ('form_conversion',
                       'chat_conversion')
ORDER BY icp_score desc, ddate DESC