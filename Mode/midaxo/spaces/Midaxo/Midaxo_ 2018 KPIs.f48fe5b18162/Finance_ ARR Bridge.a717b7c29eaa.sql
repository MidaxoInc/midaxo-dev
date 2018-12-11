with ib as
  ((SELECT a.date,
          a.metric,
          a.value,
          max(a.date) over () as maxdate
   FROM raw.manual.install_base a
   WHERE a.value is not null
   ORDER BY a.date, a.value::number DESC)
UNION
  (SELECT a.date,
          'ending_arr' as metric,
          sum(a.value) over (partition by a.date) as value,
          max(a.date) over () as maxdate
   FROM raw.manual.install_base a
   WHERE a.value is not null
   ORDER BY a.date DESC))

select  i.*,
        case  when i.metric = 'starting_arr' then 1
              when i.metric = 'direct' then 2
              when i.metric = 'partner' then 3
              when i.metric = 'engagement' then 4
              when i.metric = 'expansion' then 5
              when i.metric = 'upgrade' then 6
              when i.metric = 'churn' then 7
              when i.metric = 'fx' then 8
              when i.metric = 'ending_arr' then 9
        end as sort
from ib i
where i.maxdate=i.date
order by sort asc