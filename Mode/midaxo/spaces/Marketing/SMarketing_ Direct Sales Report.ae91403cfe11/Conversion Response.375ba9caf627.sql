WITH  conversion AS
  (SELECT *
   FROM MIDAXO.DEV.EVENT_TIMELINE a
   WHERE a.event_type in ('form_conversion',
                          'chat_conversion')
     AND last_day(a.eventdate,'week') between dateadd('week',-52,last_day(current_date,'week')) AND last_day(current_date,'week')), 
 action AS
  (SELECT a.*
   FROM MIDAXO.DEV.EVENT_TIMELINE a
   WHERE a.event_type in ('sales_email',
                          'sales_call',
                          'meeting',
                          'chat_response')
    AND last_day(a.eventdate,'week') between dateadd('week',-52,last_day(current_date,'week')) AND last_day(current_date,'week')),
       response AS
  (SELECT v.company_id,
          v.contact_id,
          c.property_name AS company_name,
          c.property_icp_score AS icp_score,
          c.property_sales_territory AS territory,
          c.property_bad_fit_company_do_not_rotate AS bad_fit,
          m.rep AS sales_rep,
          v.eventdate AS conversion_date,
          v.event_type AS conversion_type,
          v.event_source AS conversion_source,
          v.event_owner_campaign_url AS conversion_content,
          r.eventdate AS response_date,
          r.event_type AS response_type,
          r.event_owner_campaign_url AS response_owner,
          datediff('minute',v.eventdate,r.eventdate) AS response_min,
          row_number() over (partition BY v.contact_id
                           ORDER BY r.eventdate ASC) AS x
   FROM conversion v
   left join action r
     ON v.company_id = r.company_id
     AND r.eventdate >= v.eventdate
   left join raw.hubspot2.company c
     ON c.id = v.company_id
   left join midaxo.dev.mstd m
     ON r.event_owner_campaign_url = m.owner_id
     AND last_day(r.eventdate)=m.ddate
   WHERE ifnull(c.property_partner,'false') = 'false'
     AND ifnull(c.property_partner_engagement,'false') = 'false') 

SELECT r.conversion_date AS ddate,
       r.response_min,
       r.company_name,
       r.icp_score,
       r.territory,
       r.sales_rep,
       r.conversion_type,
       r.conversion_source,
       CASE
           WHEN r.response_min is null then '1. unworked'
           WHEN r.response_min <= 10 then '5. <10 min'
           WHEN r.response_min <= 60 then '4. <1 hr'
           WHEN r.response_min <= 1440 then '3. <1 day'
           ELSE '2. 1+ day'
       END AS response_bucket
FROM response r
WHERE (r.x = 1
       OR r.x is null)
  AND r.conversion_date < current_date
  and dayofweek(r.conversion_date) not in (0,6)
  and ifnull(r.bad_fit,'false') = 'false'
  and r.territory in ('nam','uk','benelux','nordics','dach','eur')
ORDER BY r.conversion_date DESC