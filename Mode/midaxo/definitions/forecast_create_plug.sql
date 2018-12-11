with datespine AS
  (SELECT a.ddate,
          last_day(a.ddate,'quarter') AS quarter
   FROM midaxo.dev.datetable_clean a
   WHERE a.ddate > dateadd('quarter',-5,dateadd('day',1,last_day(current_date,'quarter')))),
     deal AS
  (SELECT a.deal_id,
          a.createdate,
          a.closedate,
          a.deal_amount,
          CASE
              WHEN last_day(a.createdate, 'quarter') = last_day(a.closedate, 'quarter') then true
              ELSE false
          END AS create_in_quarter
   FROM MIDAXO.DEV.DEAL a
   WHERE a.pipeline_type = 'direct'
     AND a.pipeline_stage = 'closed won'
     AND createdate < closedate),
     createclose AS
  (SELECT DISTINCT last_day(d.closedate,'quarter') AS quarter,
                   sum(CASE
                           WHEN d.create_in_quarter = true then 1
                           ELSE 0
                       END) over (partition BY quarter) AS create_close,
                   count(d.deal_id) over (partition BY quarter) AS total,
                   create_close/total AS create_close_rate
   FROM deal d)
SELECT distinct d.ddate,
                sum(c.create_close)/sum(c.total) AS forecast
FROM datespine d
left join createclose c
  ON c.quarter between dateadd('day',1,last_day(dateadd('quarter',-3,d.ddate),'quarter')) AND last_day(dateadd('quarter',-1,d.ddate),'quarter')
GROUP BY d.ddate
ORDER BY d.ddate DESC