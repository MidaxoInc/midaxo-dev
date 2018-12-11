with attribution AS
  (SELECT d.closedate,
          a.dealcreatedate,
          a.company_id,
          a.deal_id,
          a.company_name,
          a.deal_name,
          d.pipeline_type,
          d.pipeline_stage,
          lower(a.territory) AS territory,
          a.country,
          a.icp_score,
          a.event_category,
          a.event_type,
          a.event_source,
          a.deal_value,
          row_number() over (partition BY a.deal_id
                           ORDER BY d.closedate) AS x
   FROM midaxo.dev.pipecreated_attr a
   left join midaxo.dev.deal d
     ON a.deal_id = d.deal_id
   WHERE d.pipeline_type = 'direct'),
     datespine AS
  (SELECT a.*
   FROM MIDAXO.DEV.DATETABLE_CLEAN a
   WHERE a.ddate between dateadd('week',-52,last_day(current_date,'week')) AND current_date)
SELECT distinct d.ddate,
                a.*
FROM datespine d
left join attribution a
  ON a.dealcreatedate = d.ddate
WHERE x = 1
  OR x is null
ORDER BY d.ddate DESC,
         deal_id