select
  a.date_day as ddate,
  extract(week , ddate) as week,
  extract(month , ddate) as month,
  extract(quarter , ddate) as quarter,
  extract(year , ddate) as year
from {{ref('DATETABLE')}} a
