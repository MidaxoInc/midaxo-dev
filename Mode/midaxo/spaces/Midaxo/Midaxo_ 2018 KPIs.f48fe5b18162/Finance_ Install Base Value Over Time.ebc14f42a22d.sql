with data AS
    (SELECT distinct last_day(to_date(a.date),'month') AS ddate,
                     sum(a.value) over (partition BY ddate) AS actual
   FROM raw.manual.install_base a
   WHERE last_day(to_date(a.date),'month') <= last_day(dateadd('month',-1,current_date),'month'))
SELECT last_day(to_date(b.date),'month') AS ddate,
       d.actual,
       b.value AS target,
       d.actual / b.value AS attainment
FROM RAW.MANUAL.KPI_TARGET b
left join data d
  ON last_day(to_date(b.date),'month') = d.ddate
WHERE b."GROUP" = 'install base'
  AND b.value > 0