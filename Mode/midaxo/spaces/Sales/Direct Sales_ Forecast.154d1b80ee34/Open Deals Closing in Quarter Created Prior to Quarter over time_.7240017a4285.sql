WITH datespine AS
  (SELECT a.ddate,
          last_day(a.ddate,'quarter') AS quarter
   FROM midaxo.dev.datetable_clean a
   WHERE a.ddate > dateadd('quarter',-5,dateadd('day',1,last_day(current_date,'quarter')))),
     opendeals AS
  (SELECT DISTINCT d.ddate,
                   a.deal_id,
                   a.pipeline_stage,
                   a.deal_amount
   FROM datespine d
   left join MIDAXO.DEV.DEAL_ARCHIVE_CLEAN a
     ON d.ddate between a.validfrom AND a.validto
   WHERE last_day(a.closedate, 'quarter') = last_day(d.ddate,'quarter')
     AND last_day(a.createdate,'quarter') < last_day(d.ddate,'quarter')
     AND a.pipeline_stage not ilike 'closed%'
     AND a.pipeline_type = 'direct')
SELECT DISTINCT a.ddate,
                a.pipeline_stage,
                count(a.deal_amount) over (partition BY a.ddate, a.pipeline_stage) AS dealcount
FROM opendeals a
ORDER BY a.ddate ASC