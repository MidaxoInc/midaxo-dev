with data AS
    (SELECT distinct last_day(to_date(a.date),'month') AS ddate,
                     a.metric,
                     sum(a.value) over (partition BY ddate, metric) AS actual
   FROM raw.manual.finances a
   WHERE last_day(to_date(a.date),'month') <= last_day(dateadd('month',-1,current_date),'month'))
SELECT last_day(to_date(b.date),'month') AS ddate,
        b.metric,
       d.actual,
       b.value AS target,
       d.actual / b.value AS attainment
FROM RAW.MANUAL.KPI_TARGET b
left join data d
  ON last_day(to_date(b.date),'month') = d.ddate
  AND b.metric = d.metric
WHERE b."GROUP" in ('revenue',
                    'expense')
  AND b.value <> 0