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
     datespine AS
    (SELECT distinct last_day(a.ddate,'month') AS ddate
   FROM midaxo.dev.datetable_clean a), 
-------------QUERY-------------
planerror AS
  (SELECT distinct p.startdate,
                   p.enddate,
                   p.sprint_id,
                   sum(c.story_points) over (partition BY p.sprint_id) AS actual,
                   p.plan,
                   abs(sum(c.story_points) over (partition BY p.sprint_id) / p.plan - 1) AS error
   FROM sprintplan p
   left join completed c
     ON c.x=1
     AND c.sprint_id = p.sprint_id
   ORDER BY p.sprint_id DESC) 
-------------SUMMARY-------------

SELECT distinct d.ddate,
        avg(p.error) over (partition by d.ddate) as errorrate
FROM datespine d
left join planerror p
  on last_day(p.enddate,'month')<=d.ddate
  and date_part('year',p.enddate) = date_part('year',d.ddate)
  and p.enddate < current_date
where date_part('year',d.ddate) = date_part('year',dateadd('month',-1,current_date))
  and d.ddate < current_date
order by d.ddate asc