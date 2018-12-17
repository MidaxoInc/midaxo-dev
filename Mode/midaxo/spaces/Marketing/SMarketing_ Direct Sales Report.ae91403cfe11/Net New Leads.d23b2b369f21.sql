with event AS
  (SELECT a.*,
          row_number() over (partition BY a.contact_id
                             ORDER BY a.eventdate ASC) AS contact_event_no
   FROM midaxo.dev.event_timeline a)
SELECT distinct to_date(e.eventdate) as ddate,
                e.company_id,
                e.contact_id,
                t.property_firstname || ' ' || t.property_lastname as contact_name,
                c.property_name as company_name,
                c.property_sales_territory as territory,
                c.property_icp_score as icp_score,
                e.event_category,
                e.event_type,
                e.event_source,
                count(e.event_id) over (partition BY ddate,e.contact_id, e.event_type, e.event_source) AS leads
FROM event e
left join raw.hubspot2.company c
  on c.id = e.company_id
left join raw.hubspot2.contact t
  on t.id = e.contact_id
WHERE last_day(e.eventdate,'month') between dateadd('month',-13,last_day(current_date,'month')) AND last_day(current_date,'month')
  AND (e.contact_event_no = 1
       OR e.event_type in ('form_conversion',
                           'chat_conversion'))
  and  e.contact_event_no = 1
  and ifnull(t.property_bad_fit_lead,'false') = 'false'
  and ifnull(c.property_partner,'false') = 'false'
   AND ifnull(c.property_partner_engagement,'false') = 'false'
 and c.property_sales_territory in ('nam','uk','benelux','nordics','dach','eur')
 and e.event_type in ('form_conversion','chat_conversion')
ORDER BY ddate desc