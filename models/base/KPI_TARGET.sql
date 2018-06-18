select
  b.*,
  a."GROUP" as type,
  a.metric as metric,
  a.value::float as goal
from raw.manual.kpi_target a
left join {{ref('DATETABLE_CLEAN')}} b
  on last_day(to_date(a.date), month) = b.ddate
