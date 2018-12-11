with attribution AS
  (SELECT a.*,
          a.deal_value * a.detail_share as attr_arr
   FROM midaxo.dev.pipecreated_attr a
   WHERE a.icp_score in ('A',
                         'B',
                         'C',
                         'D')),
     datespine AS
  (SELECT distinct last_day(d.ddate,'month') AS ddate
   FROM MIDAXO.DEV.DATETABLE_CLEAN d),
     target AS
  (SELECT distinct last_day(b.ddate,'month') AS ddate,
                   b.type,
                   b.metric,
                   sum(b.goal) over (partition BY b.type, b.metric, last_day(b.ddate,'month')) AS goal
   FROM midaxo.dev.kpi_target b
   WHERE lower(b.type) = 'icp'
     AND b.metric = 'pipecreated'),
     attributionsum AS
  (SELECT distinct d.ddate,
                   'ICP Pipeline Created' AS measure,
                   sum(a.attr_arr) over (partition BY d.ddate) AS actual
   FROM datespine d
   left join attribution a
     ON last_day(a.dealcreatedate,'month') = d.ddate)
SELECT distinct s.*,
                sum(t.goal) over (partition BY s.ddate) AS target
FROM attributionsum s
left join target t
  ON s.ddate = t.ddate
  WHERE date_part('year',s.ddate) = date_part('year',dateadd('month',-1,current_date))
ORDER BY s.ddate ASC