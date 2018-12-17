select
a.weeklabel,
a.monthlabel,
a.event_type,
count(a.eventdate) as eventcount,
count(distinct a.company_id) as unique_company_eventcount
from midaxo.dev.event_timeline a
where a.year = '2018' and a.event_type in ('form','chat') and a.company_event_no = '1'
group by a.weeklabel, a.monthlabel, a.event_type
