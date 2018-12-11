-------------CTE-------------
with sprint AS
  (SELECT a.id AS sprint_id,
          to_date(a.start_date) AS startdate,
          to_date(a.end_date) AS enddate
   FROM raw.jira.sprint a),
     completed AS
  (SELECT distinct a.ddate,
                   a.key,
                   a.issue_id,
                   a.sprint_id,
                   a.story_points,
                   row_number() over (partition BY a.issue_id, a.sprint_id
                                    ORDER BY a.ddate ASC) AS x
   FROM midaxo.dev.jira_issue_history a
   WHERE a.status_id in ('10001',
                         '6')),
     planned AS
  (SELECT distinct a.ddate,
                   a.key,
                   a.issue_id,
                   a.sprint_id,
                   a.story_points
   FROM midaxo.dev.jira_issue_history a
   inner join sprint s
     ON s.sprint_id = a.sprint_id
     AND dateadd('day',1,s.startdate) = a.ddate),
     sprintplan AS
    (SELECT distinct last_day(s.enddate,'month') as startdate,
                     last_day(s.enddate,'month') as enddate,
                     sum(p.story_points) over (partition BY last_day(s.enddate,'month')) AS plan
   FROM sprint s
   left join planned p
     ON p.sprint_id = s.sprint_id) 
-------------QUERY-------------
SELECT distinct last_day(p.enddate,'month') as enddate,
                sum(c.story_points) over (partition BY last_day(p.enddate,'month')) AS actual,
                p.plan
FROM sprintplan p
left join completed c
  ON c.x=1
  AND last_day(c.ddate,'month') = p.enddate

ORDER BY enddate DESC