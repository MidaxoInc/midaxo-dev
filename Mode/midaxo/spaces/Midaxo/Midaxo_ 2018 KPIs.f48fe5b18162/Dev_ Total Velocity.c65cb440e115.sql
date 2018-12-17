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
   WHERE c.x=1
     AND date_part('year',ddate) = date_part('year',dateadd('month',-1,current_date))),
     headcount AS (
  SELECT *
  FROM raw.manual.headcount)
------------------ QUERY ------------------    
SELECT c.*,
  h.headcount,
  c.actual / h.headcount as velocity
FROM completedsum c
left join headcount h
  on last_day(h.ddate,'month') = last_day(c.ddate,'month')
ORDER BY ddate DESC