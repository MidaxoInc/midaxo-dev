-------CTE--------
with data AS
    (SELECT distinct last_day(to_date(a.date),'month') AS ddate,
                     a.metric,
                     sum(a.value) over (partition BY ddate, metric) AS actual
   FROM raw.manual.finances a
   WHERE last_day(to_date(a.date),'month') <= last_day(dateadd('month',-1,current_date),'month')),
cumulative as 
(SELECT last_day(to_date(b.date),'month') AS ddate,
        b.metric,
       sum(d.actual) over (order by ddate asc) as actual,
       sum(b.value) over (order by ddate asc) AS target
FROM RAW.MANUAL.KPI_TARGET b
left join data d
  ON last_day(to_date(b.date),'month') = d.ddate
  AND b.metric = d.metric
WHERE b."GROUP" in ('revenue')
  AND b.value <> 0),
  datespine as 
  (select distinct last_day(d.ddate,'month') as ddate
  from MIDAXO.DEV.DATETABLE_CLEAN d)
-------QUERY--------
  select distinct d.ddate,
    sum (c.actual) over (partition by d.ddate) as actual,
    sum (c.target) over (partition by d.ddate) as target,
    sum (c.actual) over (partition by d.ddate)/sum (c.target) over (partition by d.ddate) as attainment
  from datespine d
  left join cumulative c
  on c.ddate <= d.ddate
  where date_part('year',c.ddate) = date_part('year',dateadd('month',-1,current_date))
  and d.ddate <= dateadd('month',-1,last_day(current_date))
  order by d.ddate asc