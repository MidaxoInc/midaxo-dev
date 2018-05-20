--to be joined with deal info in the pipeline view

--select only dates at the "day" level
select
  days.ddate,
  histvalue.*
from
-- select only unique days from the date table
  (select a.ddate
  from {{ref('DATETABLE')}} a
  order by ddate desc
  ) days
-- join historical deal values
left join
  (select
    b.deal_id,
    b.changedate,
    (case when b.dealproperty = 'amount' then b.value end) as dealvalue
    b.dealproperty
  from {{ref('DEAL_HISTORY')}} b
  order by b.changedate desc
  ) histvalue
    on
    (histvalue.changedate <= days.ddate)
