WITH datespine AS
  (SELECT b.ddate,
          last_day(b.ddate,'quarter') AS quarter
   FROM midaxo.dev.datetable_clean b
   WHERE b.ddate > dateadd('quarter',-5,dateadd('day',1,last_day(current_date,'quarter')))),
     deal AS
  (SELECT b.closedate,
          b.createdate,
          b.deal_id,
          b.pipeline_stage,
          b.deal_amount
   FROM midaxo.dev.deal b
   WHERE b.pipeline_stage ='closed won'
     AND last_day(b.createdate,'quarter') <= last_day(b.closedate,'quarter')
     AND b.pipeline_type = 'direct'),
     wondeals AS
  (SELECT d.ddate,
          b.deal_id,
          CASE
              WHEN last_day(b.createdate,'quarter') = last_day(b.closedate,'quarter') then 'create_close'
              WHEN last_day(b.createdate,'quarter') < last_day(b.closedate,'quarter') then 'existing'
              ELSE 'other'
          END AS status,
          b.deal_amount
   FROM datespine d
   left join deal b
     ON d.ddate>=b.closedate
     AND last_day(b.closedate,'quarter')=last_day(d.ddate,'quarter'))
SELECT DISTINCT b.ddate,
                b.status,
                count(b.deal_id) over (partition BY b.ddate, b.status) AS dealcount
FROM wondeals b
ORDER BY b.ddate DESC