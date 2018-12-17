with deal AS
  (SELECT distinct last_day(d.createdate,'month') AS createdate,
                   last_day(d.closedate,'month') AS closedate,
                   datediff('month',d.createdate,d.closedate) AS mos_to_close,
                   sum(case when d.pipeline_stage = 'closed won' then 1 else 0 end) over (partition BY last_day(d.createdate,'month'),last_day(d.closedate,'month')) AS dealcount,
                   count(d.deal_amount) over (partition BY last_day(d.createdate,'month')) AS cohort_dealcount
   FROM midaxo.dev.deal d
   WHERE d.pipeline_type = 'direct'),
     datespine AS
  (SELECT distinct last_day(a.ddate,'month') AS ddate
   FROM MIDAXO.DEV.DATETABLE_CLEAN a
   WHERE a.ddate between dateadd('month',-6,current_date) AND dateadd('month',6,current_date)),
     datecohort AS
  (SELECT d.*,
          datediff('month',d.ddate,c.ddate) AS mos_to_close
   FROM datespine d
   left join datespine c
     ON datediff('month',d.ddate,c.ddate) between 0 AND 6)
SELECT d.ddate AS createdate,
       d.mos_to_close,
       a.cohort_dealcount as deals,
       a.dealcount,
       ifnull(a.dealcount/a.cohort_dealcount,0) AS deals_won
FROM datecohort d
left join deal a
  ON a.createdate = d.ddate
  AND a.mos_to_close = d.mos_to_close
where d.ddate <= last_day(current_date,'month')
ORDER BY d.ddate ASC,
         d.mos_to_close ASC