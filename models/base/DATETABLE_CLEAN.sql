select
  b.*,
  --combine year and week, and move all data from a 52nd or 53rd week in Jan into the end of the prev. year
  (case
    when (b.month = '1' and b.week > '51')
    then concat(cast(b.year - '1' as varchar), cast(b.week as varchar(2)))
    else concat(cast(b.year as varchar), right(concat('0', cast(b.week as varchar(2))),2))
    end
  ) as yearweek,
  dateadd(day,datediff(week,'0',b.ddate),'0') as weeklabel,
  concat(cast(b.year as varchar), right(concat('0', cast(b.month as varchar(2))),2)) as yearmonth,
  concat(cast(b.year as varchar), right(concat('0', cast(b.month as varchar(2))),2)) as yearmonth,
  concat('WE: ', dateadd(day,'-1',last_day(b.ddate,week))) as weeklabel,
  concat(b.year,concat('-',monthname(b.ddate))) as monthlabel
from (
  select
    a.date_day as ddate,
    extract(week , ddate) as week,
    extract(month , ddate) as month,
    extract(quarter , ddate) as quarter,
    extract(year , ddate) as year
  from {{ref('DATETABLE')}} a
) b
