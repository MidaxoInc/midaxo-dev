select
a.weeklabel,
a.yearmonth,
a.monthlabel,
a.event_type,
a.event_source,
count(concat(a.contact_id, a.contact_event_no)) as eventcount,
count(distinct a.company_id) as unique_company_eventcount
from midaxo.dev.event_timeline a
where a.event_type in ('form','chat') 
  and a.company_event_no = '1'
  and a.year = '2018'
group by a.weeklabel, a.yearmonth, a.monthlabel, a.event_type, a.event_source
order by eventcount desc
