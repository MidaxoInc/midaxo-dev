with deal AS
  (SELECT a.deal_id,
          a.createdate,
          a.closedate,
          a.pipeline_stage,
          datediff('day',a.createdate,a.closedate) AS age
   FROM midaxo.dev.deal a
   WHERE a.pipeline_type = 'direct')
SELECT last_day(d.closedate,'quarter') AS quarter,
       sum(CASE
               WHEN (last_day(d.createdate,'quarter') = last_day(d.closedate,'quarter')
                    AND d.pipeline_stage = 'closed won') then 1
               ELSE 0
           END)/nullif(sum(case when d.pipeline_stage = 'closed won' then 1 else 0 end),0) AS inquarter,
       avg(case when d.pipeline_stage = 'closed won' then d.age else null end) AS avg_closedwon_age,
       avg(d.age) AS avg_pipeline_age
FROM deal d
WHERE d.closedate between 
  dateadd('quarter',-10,dateadd('day',1,last_day(current_date,'quarter'))) 
  and last_day(current_date,'quarter')
GROUP BY quarter
ORDER BY quarter DESC