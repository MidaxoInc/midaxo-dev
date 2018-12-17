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
   inner join sprint s
     ON s.sprint_id = a.sprint_id
   WHERE a.status_id in ('10001',
                         '6')
     AND a.ddate between s.startdate AND s.enddate),
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
    (SELECT distinct s.startdate,
                     s.enddate,
                     s.sprint_id,
                     sum(p.story_points) over (partition BY s.sprint_id) AS plan
   FROM sprint s
   left join planned p
     ON p.sprint_id = s.sprint_id), 
     -------------QUERY-------------
data as
(SELECT distinct p.startdate,
                p.enddate,
                p.sprint_id,
                sum(c.story_points) over (partition BY p.sprint_id) AS actual,
                p.plan
FROM sprintplan p
left join completed c
  ON c.x=1
  AND c.sprint_id = p.sprint_id
ORDER BY p.sprint_id DESC)

select distinct last_day(d.enddate,'month') as enddate,
                sum(d.actual) over (partition by last_day(d.enddate,'month')) as actual,
                sum(d.plan) over (partition by last_day(d.enddate,'month')) as plan
from data d