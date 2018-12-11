with icp AS
  (SELECT a.*
   FROM midaxo.dev.pipewon_attr a
   WHERE a.icp_score in ('A',
                         'B',
                         'C',
                         'D')),
   inbound AS
  (SELECT a.*
   FROM midaxo.dev.pipewon_inbound a
   WHERE a.icp_score not in ('A',
                         'B',
                         'C',
                         'D')),
   attribution as
   (select * from icp
   union all
   select * from inbound),
     datespine AS
  (SELECT distinct last_day(d.ddate,'month') AS ddate
   FROM MIDAXO.DEV.DATETABLE_CLEAN d),
     target AS
  (SELECT distinct last_day(b.ddate,'month') AS ddate,
                   sum(b.goal) over (partition BY last_day(b.ddate,'month')) AS goal
   FROM midaxo.dev.kpi_target b
   WHERE (lower(b.type) = 'icp'
          OR lower(b.type) = 'inbound')
     AND b.metric = 'pipewon'),
     attributionsum AS
  (SELECT distinct d.ddate,
                   sum(a.attributed_pipeline_won) over (partition BY d.ddate) AS actual
   FROM datespine d
   left join attribution a
     ON last_day(a.dealclosedate,'month') = d.ddate)
--QUERY--
SELECT distinct t.ddate,
                sum(s.actual) over (ORDER BY t.ddate ASC) AS actual,
                sum(t.goal) over (ORDER BY t.ddate ASC) AS target,
                sum(s.actual) over (ORDER BY t.ddate ASC) / sum(t.goal) over (ORDER BY t.ddate ASC) AS attainment
FROM target t
left join attributionsum s
  ON s.ddate = t.ddate
WHERE date_part('year',t.ddate) = date_part('year',last_day(dateadd('month',-1,current_date),'month'))
  AND t.ddate <= last_day(dateadd('month',-1,current_date),'month')
  AND t.goal > 0
ORDER BY t.ddate ASC