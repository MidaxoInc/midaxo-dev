with attribution AS
  (SELECT d.closedate,
          a.dealcreatedate,
          a.company_id,
          a.deal_id,
          a.company_name,
          a.deal_name,
          d.pipeline_type,
          d.pipeline_stage,
          lower(a.territory) as territory,
          a.country,
          a.icp_score,
          a.event_category,
          a.event_type,
          a.event_source,
          sum(a.detail_share) over (partition by a.deal_id, a.event_category, a.event_type, a.event_source) as detail_share,
          a.deal_value,
          row_number() over (partition by a.deal_id, a.event_category, a.event_type, a.event_source order by d.closedate) as x
   FROM midaxo.dev.pipecreated_attr a
   left join midaxo.dev.deal d
     ON a.deal_id = d.deal_id
     WHERE d.pipeline_type = 'direct'
      and d.pipeline_stage = 'closed won'),
     datespine AS
    (SELECT a.*
   FROM MIDAXO.DEV.DATETABLE_CLEAN a
   WHERE last_day(a.ddate,'week') between dateadd('week',-52,last_day(current_date,'week')) AND last_day(current_date,'week'))
SELECT distinct d.ddate,
                a.*,
                a.detail_share * a.deal_value as arr_won
FROM datespine d
left join attribution a
  ON to_date(a.closedate) = d.ddate
where x = 1 or x is null
ORDER BY d.ddate DESC, deal_id