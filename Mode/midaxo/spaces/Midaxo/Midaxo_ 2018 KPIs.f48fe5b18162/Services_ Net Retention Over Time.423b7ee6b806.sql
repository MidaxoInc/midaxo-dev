--CTE--
with churn AS
    (SELECT distinct to_date(a.date) AS ddate,
                     sum(a.value) over (partition BY a.date) AS value
   FROM raw.manual.install_base a
   WHERE a.value is not null
     AND a.metric in ('churn',
                      'upgrade',
                      'expansion')
   ORDER BY ddate ASC),
     monthly AS
  (SELECT c.ddate,
          c.value AS cum_arr,
          CASE
              WHEN row_number() over (
                                      ORDER BY c.ddate ASC) = 1 then c.value
              ELSE c.value - sum(c.value) over (
                                                ORDER BY c.ddate ASC rows between 1 preceding AND 1 preceding)
          END AS arr
   FROM churn c),
     target AS
  (SELECT  to_date(a.date) as ddate,
                   a."GROUP" AS metric,
                   a.metric AS measure,
                   sum(a.value) over (partition BY a.date) AS target
   FROM RAW.MANUAL.KPI_TARGET a
   WHERE a."GROUP" in ('net retention')) 
                     
--QUERY--

SELECT distinct last_day(t.ddate,'quarter') AS ddate,
                t.metric,
                sum(m.arr) over (partition BY t.metric, last_day(t.ddate,'quarter')) AS arr,
                sum(t.target) over (partition BY t.metric, last_day(t.ddate,'quarter')) AS target
FROM target t
left join monthly m
  on t.ddate = m.ddate