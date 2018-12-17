with arr AS
  (SELECT distinct last_day(a.closedate,'month') AS ddate,
                   a.pipeline_type,
                   sum(a.recognized_arr) over (partition BY last_day(a.closedate,'month'), a.pipeline_type) / '12' AS net_bookings
   FROM midaxo.dev.arr a
   WHERE a.pipeline_type = 'engagement'
     AND last_day(a.closedate) <= last_day(current_date)
   ORDER BY ddate ASC),
     arr_target AS
  (SELECT distinct last_day(b.ddate,'quarter') AS ddate,
                   lower(b.type) AS pipeline_type,
                   b.metric,
                   sum(b.goal) over (partition BY lower(b.type), last_day(b.ddate,'quarter')) AS goal
   FROM midaxo.dev.kpi_target b
   WHERE pipeline_type = 'engagement'),
     mo_bookings AS
  (SELECT distinct arr.ddate,
                   sum(arr.net_bookings) over(
                                              ORDER BY arr.ddate ASC) AS mo_bookings
   FROM arr
   ORDER BY arr.ddate ASC)
SELECT distinct arr_target.ddate,
                arr_target.goal AS target,
                sum(mo_bookings.mo_bookings) over (partition BY arr_target.ddate) AS "Bookings",
                sum(mo_bookings.mo_bookings) over (partition BY arr_target.ddate)/arr_target.goal AS "Attainment"
FROM arr_target
left join mo_bookings
  ON last_day(mo_bookings.ddate,'quarter') = arr_target.ddate
WHERE date_part('year',arr_target.ddate) = '2018'
ORDER BY arr_target.ddate ASC