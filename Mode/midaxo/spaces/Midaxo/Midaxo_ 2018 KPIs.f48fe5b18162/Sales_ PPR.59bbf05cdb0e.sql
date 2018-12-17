--CTE--
with arr AS
  (SELECT distinct last_day(a.closedate,'month') AS ddate,
                   a.owner_id,
                   sum(a.recognized_arr) over (partition BY last_day(a.closedate,'month'), a.owner_id) AS arr
   FROM midaxo.dev.arr a),
     reps AS
  (SELECT last_day(a.ddate,'month') as ddate,
          a.owner_id
   FROM midaxo.dev.mstd a
   WHERE a.team in ('Direct',
                    'Partner',
                    'Expansion')),
 pprmo as
(SELECT distinct last_day(r.ddate,'month') as ddate,
                count(r.owner_id) over (partition BY last_day(r.ddate,'month')) AS reps,
                sum(a.arr) over (partition BY last_day(r.ddate,'month')) AS arr,
                sum(a.arr) over (partition BY last_day(r.ddate,'month')) / count(r.owner_id) over (partition BY last_day(r.ddate,'month')) AS ppr
FROM reps r
left join arr a
  ON r.ddate = a.ddate
  AND r.owner_id = a.owner_id
   
ORDER BY last_day(r.ddate,'month') ASC)

select  p.ddate,
        sum(p.reps) over (order by p.ddate asc rows between 2 preceding and current row) as cumreps,
        sum(p.arr) over (order by p.ddate asc rows between 2 preceding and current row) as cumarr,
        cumarr / cumreps * 3 as ppr
from pprmo p
WHERE last_day(p.ddate,'month') <= last_day(dateadd('month',-1,current_date),'month')
     AND date_part('year',p.ddate)=date_part('year',dateadd('month',-1,current_date))