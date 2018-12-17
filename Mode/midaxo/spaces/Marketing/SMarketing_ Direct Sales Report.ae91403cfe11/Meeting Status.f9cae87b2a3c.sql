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
     created AS
  (SELECT a.deal_id,
          a.company_id,
          a.owner_id,
          a.pipeline_type,
          a.pipeline_stage AS created,
          to_date(a.validfrom) AS validfrom,
          row_number() over (partition BY a.deal_id
                             ORDER BY a.validfrom ASC) AS firstrow
   FROM MIDAXO.DEV.DEAL_ARCHIVE_CLEAN a
   where a.pipeline_type = 'direct'),
     qualified AS
  (SELECT a.deal_id,
          a.company_id,
          a.pipeline_type,
          a.pipeline_stage AS qualified,
          to_date(a.validfrom) AS validfrom,
          row_number() over (partition BY a.deal_id
                             ORDER BY a.validfrom ASC) AS firstrow
   FROM MIDAXO.DEV.DEAL_ARCHIVE_CLEAN a
   where a.pipeline_stage not in ('activation','nurture','qualification','closed lost')
   and a.pipeline_type = 'direct'),
     currentstage AS
  (SELECT a.deal_id,
          a.company_id,
          a.closedate,
          a.pipeline_type,
          a.pipeline_stage
   FROM MIDAXO.DEV.DEAL a),
     datespine AS
  (SELECT a.*
   FROM MIDAXO.DEV.DATETABLE_CLEAN a
   WHERE last_day(a.ddate,'week') between dateadd('week',-52,last_day(current_date,'week')) AND last_day(current_date,'week'))
SELECT distinct d.ddate,
                q.validfrom as qualification_date,
                datediff('days',d.ddate,ifnull(q.validfrom,current_date)) as days_to_qualify,
                s.closedate,
                m.rep AS sdr_name,
                a.rep AS ae_name,
                p.property_name AS company_name,
                p.property_icp_score AS icp_score,
                p.property_sales_territory AS territory,
                c.deal_id,
                s.pipeline_stage AS current_stage,
                CASE
                    WHEN q.validfrom is not null then 'qualified'
                    WHEN b.meetingdate > dateadd('day',-7,current_date) then 'open'
                    WHEN s.pipeline_stage ='qualification' then 'open'
                    when a.role = 'Account Executive' then 'disqualified'
                    WHEN s.pipeline_stage ='closed lost' then 'disqualified'
                    else 'disqualified'
                END AS status
FROM booked b
left join datespine d
  ON d.ddate = b.meetingdate
left join created c
  ON c.validfrom between dateadd('day',-14,b.meetingdate) AND dateadd('day',90,b.meetingdate)
AND c.company_id = b.company_id
AND c.firstrow = 1
left join qualified q
  ON c.deal_id = q.deal_id
  AND q.firstrow = 1
left join raw.hubspot2.company p
  ON p.id=b.company_id
left join midaxo.dev.mstd m
  ON m.owner_id = b.rep
  AND m.ddate = last_day(d.ddate,'month')
left join midaxo.dev.mstd a
  ON a.owner_id = c.owner_id
  AND a.ddate = last_day(d.ddate,'month')
left join currentstage s
  ON s.deal_id = c.deal_id

ORDER BY ddate DESC, status DESC