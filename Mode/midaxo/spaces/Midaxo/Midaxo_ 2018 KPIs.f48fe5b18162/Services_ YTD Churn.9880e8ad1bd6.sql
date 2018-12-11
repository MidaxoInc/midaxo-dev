--CTE--
with churn AS
    (SELECT distinct to_date(a.date) AS ddate,
                     sum(a.value) over (partition BY a.date) AS value
   FROM raw.manual.install_base a
   WHERE a.value is not null
     AND a.metric in ('churn')
   ORDER BY ddate ASC),
     monthly AS
  (SELECT distinct c.ddate,
          c.value AS cum_arr,
          CASE
              WHEN row_number() over (ORDER BY c.ddate ASC) = 1 then c.value
              ELSE c.value - sum(c.value) over (ORDER BY c.ddate ASC rows between 1 preceding AND 1 preceding)
          END AS arr
   FROM churn c),
     target AS
  (SELECT distinct to_date(a.date) as ddate,
                   sum(a.value) over (partition BY a.date) AS target
   FROM RAW.MANUAL.KPI_TARGET a
   WHERE a."GROUP" in ('churn')) 
                     
--QUERY--

SELECT distinct t.ddate,
                sum(m.arr) over (order BY t.ddate asc rows between unbounded preceding and current row) AS arr,
                sum(t.target) over (order BY t.ddate asc rows between unbounded preceding and current row) AS target,
                sum(m.arr) over (order BY t.ddate asc rows between unbounded preceding and current row) - sum(t.target) over (order BY t.ddate asc rows between unbounded preceding and current row) as net
FROM target t
left join monthly m
  on t.ddate = m.ddate
where m.arr is not null
order by t.ddate asc
