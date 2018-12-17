
with attribution AS
  (SELECT a.*
   FROM midaxo.dev.pipewon_inbound a),
     datespine AS
  (SELECT distinct last_day(d.ddate,'quarter') AS ddate
   FROM MIDAXO.DEV.DATETABLE_CLEAN d),
     target AS
  (SELECT distinct last_day(b.ddate,'quarter') AS ddate,
                   b.type,
                   b.metric,
                   sum(b.goal) over (partition BY b.type, b.metric, last_day(b.ddate,'month')) AS goal
   FROM midaxo.dev.kpi_target b
   WHERE lower(b.type) = 'inbound'
     AND b.metric = 'pipewon'),
     attributionsum AS
  (SELECT distinct d.ddate,
                   'Inbound Pipeline Won' AS measure,
                   sum(a.attributed_pipeline_won) over (partition BY d.ddate) AS actual
   FROM datespine d
   left join attribution a
     ON last_day(a.dealclosedate,'quarter') = d.ddate)
SELECT distinct s.*,
                sum(t.goal) over (partition BY s.ddate) AS target
FROM attributionsum s
left join target t
  ON s.ddate = t.ddate
  WHERE date_part('year',s.ddate) = date_part('year',dateadd('month',-1,current_date))
ORDER BY s.ddate ASC