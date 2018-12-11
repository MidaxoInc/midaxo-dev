with booked AS
  (SELECT to_date(a.eventdate) AS meetingdate,
          a.engagement_id,
          a.company_id,
          a.contact_id,
          a.event_source,
          a.event_owner_campaign_url AS rep
   FROM MIDAXO.DEV.EVENT_TIMELINE a
   WHERE a.event_type in ('meeting')
     AND a.event_source='sdr'
     AND last_day(a.eventdate,'week') between dateadd('week',-52,last_day(current_date,'week')) AND last_day(current_date,'week')),
     conversions AS
  (SELECT to_date(a.eventdate) AS conversiondate,
          a.engagement_id,
          a.company_id,
          a.contact_id,
          a.event_source,
          a.event_type,
          a.event_owner_campaign_url AS content
   FROM MIDAXO.DEV.EVENT_TIMELINE a
   WHERE a.event_type in ('chat_conversion',
                          'form_conversion')
     AND last_day(a.eventdate,'week') between dateadd('week',-52,last_day(current_date,'week')) AND last_day(current_date,'week')),
     created AS
  (SELECT a.deal_id,
          a.company_id,
          a.owner_id,
          a.pipeline_type,
          a.pipeline_stage AS created,
          to_date(a.validfrom) AS validfrom,
          row_number() over (partition BY a.deal_id
                             ORDER BY a.validfrom ASC) AS firstrow
   FROM MIDAXO.DEV.DEAL_ARCHIVE_CLEAN a),
     qualified AS
  (SELECT a.deal_id,
          a.company_id,
          a.pipeline_type,
          a.pipeline_stage AS qualified,
          to_date(a.validfrom) AS validfrom,
          row_number() over (partition BY a.deal_id
                             ORDER BY a.validfrom ASC) AS firstrow
   FROM MIDAXO.DEV.DEAL_ARCHIVE_CLEAN a
   WHERE a.pipeline_stage in ('discovery',
                              'demo')),
     currentstage AS
  (SELECT a.deal_id,
          a.company_id,
          a.pipeline_type,
          a.pipeline_stage
   FROM MIDAXO.DEV.DEAL a),
     datespine AS
  (SELECT a.*
   FROM MIDAXO.DEV.DATETABLE_CLEAN a
   WHERE last_day(a.ddate,'week') between dateadd('week',-52,last_day(current_date,'week')) AND last_day(current_date,'week')),
     meetings AS
  (SELECT distinct d.ddate,
                   b.*,
                   m.rep AS sdr_name,
                   a.rep AS ae_name,
                   p.property_name AS company_name,
                   p.property_icp_score AS icp_score,
                   p.property_sales_territory AS territory,
                   x.property_firstname || ' ' || x.property_lastname AS contact_name,
                   c.deal_id,
                   s.pipeline_stage AS current_stage,
                   CASE
                       WHEN q.validfrom is not null then 'qualified'
                       WHEN s.pipeline_stage ='closed lost' then 'disqualified'
                       WHEN s.pipeline_stage ='qualification' then 'open'
                       WHEN b.meetingdate > dateadd('day',-30,current_date) then 'open'
                       WHEN b.meetingdate < dateadd('day',-30,current_date) then 'disqualified'
                   END AS status
   FROM booked b
   left join datespine d
     ON d.ddate = b.meetingdate
   left join created c
     ON c.validfrom between b.meetingdate AND dateadd('day',90,b.meetingdate)
   AND c.company_id = b.company_id
   AND c.firstrow = 1
   left join qualified q
     ON c.deal_id = q.deal_id
     AND q.firstrow = 1
   left join raw.hubspot.company p
     ON p.id=b.company_id
   left join raw.hubspot.contact x
     ON x.id=b.contact_id
   left join midaxo.dev.mstd m
     ON m.owner_id = b.rep
     AND m.ddate = last_day(d.ddate,'month')
   left join midaxo.dev.mstd a
     ON a.owner_id = c.owner_id
     AND a.ddate = last_day(d.ddate,'month')
   left join currentstage s
     ON s.deal_id = c.deal_id)
SELECT r.status,
       r.ddate,
       r.meetingdate,
       r.engagement_id,
       r.company_id,
       r.contact_id,
       r.deal_id,
       r.sdr_name,
       r.ae_name,
       r.company_name,
       r.contact_name,
       r.icp_score,
       r.territory,
       r.current_stage,
       c.conversiondate,
       c.event_source,
       c.event_type,
       c.content
FROM meetings r
left join conversions c
  ON r.company_id = c.company_id
  AND datediff('day', c.conversiondate, r.ddate) between 0 AND 30
ORDER BY r.status,
         r.ddate DESC,
         r.company_id,
         c.conversiondate DESC