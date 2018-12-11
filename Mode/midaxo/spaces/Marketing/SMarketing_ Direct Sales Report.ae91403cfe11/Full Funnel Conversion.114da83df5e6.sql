--NOTE: Database is based on current company attributes, not on attributes at the time
--CTE--
with prospect AS
  (SELECT c.id AS company_id,
          c.property_createdate AS createdate,
          c.property_sales_territory AS territory,
          c.property_icp_score AS icp_score,
          c.property_name AS company_name
   FROM raw.hubspot2.company c
   WHERE ifnull(c.property_lifecyclestage,'lead') not in ('customer')),
     engagement AS
  (SELECT e.event_id,
          e.contact_id,
          e.company_id,
          last_day(e.eventdate,'week') AS eventdate,
          e.event_type
   FROM midaxo.dev.event_timeline e),
     meeting AS
  (SELECT e.event_id AS meeting_id,
          e.contact_id,
          e.company_id,
          last_day(e.eventdate,'week') AS eventdate,
          e.event_type
   FROM midaxo.dev.event_timeline e
   WHERE e.event_type in ('meeting')
     AND lower(e.event_source) in ('sdr')),
     active_deal AS
  (SELECT d.deal_id,
          d.company_id,
          d.validfrom,
          d.validto,
          d.pipeline_stage
   FROM MIDAXO.DEV.DEAL_ARCHIVE_CLEAN d
   WHERE d.pipeline_stage not in ('closed lost',
                                  'closed won',
                                  'activation',
                                  'nurture')
     AND d.pipeline_type in ('direct')),
     datespine AS
  (SELECT last_day(d.ddate,'week') as ddate
   FROM MIDAXO.DEV.DATETABLE_CLEAN d
   WHERE last_day(d.ddate,'week') between dateadd('week',-52,last_day(current_date,'week')) AND dateadd('week',-1,last_day(current_date,'week'))),
     sub AS
  (SELECT d.ddate,
          p.company_id,
          e.event_id,
          m.meeting_id,
          e.contact_id,
          a.deal_id,
          p.company_name,
          p.territory,
          p.icp_score,
          CASE
              WHEN p.company_id is not null then 1
              ELSE 0
          END AS company,
          CASE
              WHEN e.event_id is not null then 1
              WHEN a.deal_id is not null then 1
              ELSE 0
          END AS engaged,
          CASE
              WHEN m.meeting_id is not null then 1
              ELSE 0
          END AS meeting,
          CASE
              WHEN a.deal_id is not null then 1
              ELSE 0
          END AS deal,
          row_number() over (partition BY d.ddate, p.company_id
                             ORDER BY iff(e.event_id is null, 1, 0) + iff(a.deal_id is null, 1, 0) + iff(m.meeting_id is null, 1, 0) DESC) AS dupes
   FROM datespine d
   left join prospect p
     ON p.createdate <= d.ddate
   left join engagement e
     ON e.company_id = p.company_id
     AND e.eventdate = d.ddate
   left join meeting m
     ON m.company_id = p.company_id
     AND m.eventdate = d.ddate
   left join active_deal a
     ON a.company_id = p.company_id
     AND d.ddate between a.validfrom AND a.validto)
--QUERY--     
SELECT distinct r.ddate,
                r.territory,
                r.icp_score,
                sum(r.company) over (partition BY r.ddate, r.territory, r.icp_score) AS total,
                sum(r.engaged) over (partition BY r.ddate, r.territory, r.icp_score) AS engaged,
                sum(r.deal) over (partition BY r.ddate, r.territory, r.icp_score) AS deals,
                sum(r.meeting) over (partition BY r.ddate, r.territory, r.icp_score
                                     ORDER BY r.ddate) AS meeting
FROM sub r
WHERE r.dupes = 1
  AND r.ddate < current_date