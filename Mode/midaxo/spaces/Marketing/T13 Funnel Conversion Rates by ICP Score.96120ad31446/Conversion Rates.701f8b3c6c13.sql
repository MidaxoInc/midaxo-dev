with companies AS
  (SELECT distinct c.id AS company_id,
                   ifnull(c.property_icp_score,'Non-ICP') AS icp_score
   FROM raw.hubspot2.company c
   WHERE c.property_createdate < dateadd('week',-1,last_day(current_date,'week'))),
     engaged AS
  (SELECT distinct e.company_id
   FROM midaxo.dev.event_timeline e
   left join raw.hubspot2.company c
     ON c.id = e.company_id
     WHERE e.eventdate between dateadd('week',-13,last_day(current_date,'week')) AND dateadd('week',-1,last_day(current_date,'week'))),
     activedeal AS
  (SELECT distinct a.company_id
   FROM MIDAXO.DEV.DEAL_ARCHIVE_CLEAN a
   WHERE a.pipeline_stage not in ('closed lost',
                                  'closed won',
                                  'activation',
                                  'nurture')
     AND a.pipeline_type in ('direct')
     AND a.validfrom between dateadd('week',-13,last_day(current_date,'week')) AND dateadd('week',-1,last_day(current_date,'week'))),
     qualified AS
  (SELECT distinct a.company_id
   FROM MIDAXO.DEV.DEAL_ARCHIVE_CLEAN a
   WHERE a.pipeline_stage in ('discovery',
                              'demo',
                              'evaluation',
                              'signatures')
     AND a.pipeline_type in ('direct')
     AND a.validfrom between dateadd('week',-13,last_day(current_date,'week')) AND dateadd('week',-1,last_day(current_date,'week'))),
     won AS
  (SELECT distinct a.company_id,
                   a.company_name
   FROM MIDAXO.DEV.DEAL_ARCHIVE_CLEAN a
   WHERE a.pipeline_stage in ('closed won')
     AND a.pipeline_type in ('direct')
     AND a.closedate between dateadd('week',-13,last_day(current_date,'week')) AND dateadd('week',-1,last_day(current_date,'week'))),
     lost AS
  (SELECT distinct a.company_id
   FROM MIDAXO.DEV.DEAL_ARCHIVE_CLEAN a
   WHERE a.pipeline_stage in ('closed lost')
     AND a.pipeline_type in ('direct')
     AND a.closedate between dateadd('week',-13,last_day(current_date,'week')) AND dateadd('week',-1,last_day(current_date,'week'))),
     totals AS
  (SELECT distinct dateadd('week',-1,last_day(current_date,'week')) AS ddate,
                   t.icp_score,
                   count(t.company_id) over (partition BY ddate,t.icp_score) AS total,
                   count(e.company_id) over (partition BY ddate,t.icp_score) AS engaged,
                   count(a.company_id) over (partition BY ddate,t.icp_score) AS active_deal,
                   count(q.company_id) over (partition BY ddate,t.icp_score) AS qualified_deal,
                   count(w.company_id) over (partition BY ddate,t.icp_score) AS won,
                   count(l.company_id) over (partition BY ddate,t.icp_score) AS lost
   FROM companies t
   left join engaged e
     ON e.company_id = t.company_id
   left join activedeal a
     ON a.company_id = t.company_id
   left join qualified q
     ON q.company_id = t.company_id
   left join won w
     ON w.company_id = t.company_id
   left join lost l
     ON l.company_id = t.company_id),
     rates AS
    (SELECT t.ddate,
            t.icp_score,
            t.engaged / t.total AS engagement_rate,
            t.active_deal / t.engaged AS activation_rate,
            t.qualified_deal / t.active_deal AS qualification_rate,
            t.won / t.active_deal AS active_to_close_rate,
            t.won / (t.won + t.lost) AS win_rate
   FROM totals t),
     pivot AS
  (SELECT r.*
   FROM rates r unpivot (rate
                         for measure in (engagement_rate, activation_rate, qualification_rate, active_to_close_rate, win_rate)))
SELECT p.*,
       CASE
           WHEN lower(p.measure) in ('engagement_rate') then 1
           WHEN lower(p.measure) in ('activation_rate') then 2
           WHEN lower(p.measure) in ('qualification_rate') then 3
           WHEN lower(p.measure) in ('active_to_close_rate') then 4
           WHEN lower(p.measure) in ('win_rate') then 5
       END AS sort_order,
       sort_order ||'-'||p.measure AS conversion_type
FROM pivot p
ORDER BY sort_order ASC