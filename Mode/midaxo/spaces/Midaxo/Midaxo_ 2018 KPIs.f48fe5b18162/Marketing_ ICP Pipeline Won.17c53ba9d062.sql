with attribution AS
  (SELECT a.*,
          d.closedate,
          a.deal_value * a.detail_share as attr_arr
   FROM midaxo.dev.pipecreated_attr a
    left join midaxo.dev.deal d
      on a.deal_id = d.deal_id
   WHERE a.icp_score in ('A',
                         'B',
                         'C',
                         'D')
    and d.pipeline_stage = 'closed won'),
     datespine AS
  (SELECT distinct last_day(d.ddate,'quarter') AS ddate
   FROM MIDAXO.DEV.DATETABLE_CLEAN d),
     target AS
  (SELECT distinct last_day(b.ddate,'quarter') AS ddate,
                   b.type,
                   b.metric,
                   sum(b.goal) over (partition BY b.type, b.metric, last_day(b.ddate,'quarter')) AS goal
   FROM midaxo.dev.kpi_target b
   WHERE lower(b.type) = 'icp'
     AND b.metric = 'pipewon'),
     attributionsum AS
  (SELECT distinct d.ddate,
                   'ICP Pipeline Won' AS measure,
                   sum(a.attr_arr) over (partition BY d.ddate) AS actual
   FROM datespine d
   left join attribution a
     ON last_day(a.closedate,'quarter') = d.ddate)
SELECT distinct s.*,
                sum(t.goal) over (partition BY s.ddate) AS target
FROM attributionsum s
left join target t
  ON s.ddate = t.ddate
  WHERE date_part('year',s.ddate) = date_part('year',dateadd('month',-1,current_date))
ORDER BY s.ddate ASC