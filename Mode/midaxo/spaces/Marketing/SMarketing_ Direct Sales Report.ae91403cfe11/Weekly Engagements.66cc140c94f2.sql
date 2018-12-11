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
          e.event_type,
          1 as x
   FROM midaxo.dev.event_timeline e),
     datespine AS
  (SELECT last_day(d.ddate,'week') as ddate
   FROM MIDAXO.DEV.DATETABLE_CLEAN d
   WHERE last_day(d.ddate,'week') between dateadd('week',-52,last_day(current_date,'week')) AND dateadd('week',-1,last_day(current_date,'week'))),
     sub AS
  (SELECT d.ddate,
          p.company_id,
          e.event_id,
          e.contact_id,
          p.company_name,
          p.territory,
          p.icp_score,
          e.event_type,
          CASE
              WHEN p.company_id is not null then 1
              ELSE 0
          END AS company,
          CASE
              WHEN e.event_id is not null then 1
              ELSE 0
          END AS engaged,
          row_number() over (partition BY d.ddate, p.company_id, e.event_type
                             ORDER BY iff(e.event_id is null, 1, 0) DESC) AS dupes
   FROM datespine d
   left join prospect p
     ON p.createdate <= d.ddate
   left join engagement e
     ON e.company_id = p.company_id
     AND e.eventdate = d.ddate
     and x = 1)
SELECT distinct r.ddate,
                r.territory,
                r.icp_score,
                r.event_type,
                sum(r.company) over (partition BY r.ddate, r.territory, r.icp_score, r.event_type) AS total,
                sum(r.engaged) over (partition BY r.ddate, r.territory, r.icp_score, r.event_type) AS engaged
FROM sub r
WHERE r.dupes = 1
  AND r.ddate < current_date
  order by r.ddate desc