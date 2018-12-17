------------------ TEMP TABLE ------------------    
  CREATE TEMPORARY TABLE raw.manual.headcount( ddate date, headcount number);
  INSERT INTO raw.manual.headcount
    VALUES
    (to_date('2018-01-01'),13),
    (to_date('2018-02-01'),15),
    (to_date('2018-03-01'),15),
    (to_date('2018-04-01'),16),
    (to_date('2018-05-01'),16),
    (to_date('2018-06-01'),16),
    (to_date('2018-07-01'),16),
    (to_date('2018-08-01'),16),
    (to_date('2018-09-01'),17),
    (to_date('2018-10-01'),16),
    (to_date('2018-11-01'),17),
    (to_date('2018-12-01'),17);
------------------ CTE ------------------    
with completed AS
  (SELECT a.*,
          row_number() over (partition BY a.issue_id
                             ORDER BY a.ddate ASC) AS x
   FROM midaxo.dev.jira_issue_history a
   WHERE a.status_id in ('10001',
                         '6')),
     completedsum AS
  (SELECT distinct last_day(c.ddate,'month') AS ddate,
                   sum (c.story_points) over (partition BY last_day(c.ddate,'month')) AS actual
   FROM completed c
   WHERE c.x=1),
     headcount AS 
  (SELECT *
  FROM raw.manual.headcount),
    datespine as
  (select last_day(d.ddate,'month') as ddate
  from MIDAXO.DEV.DATETABLE_CLEAN d)
------------------ QUERY ------------------    
SELECT distinct d.ddate,
  sum(c.actual) over (partition by d.ddate) / sum(h.headcount) over (partition by d.ddate) as velocity
FROM datespine d 
left join completedsum c
  on last_day(c.ddate,'month') <= d.ddate
  and date_part('year',c.ddate) = date_part('year',d.ddate)
left join headcount h
  on last_day(h.ddate,'month') = last_day(c.ddate,'month')
where date_part('year',d.ddate) = date_part('year',dateadd('month',-1,current_date))
  and last_day(d.ddate) <= last_day(dateadd('month',-1,current_date),'month')
ORDER BY d.ddate asc