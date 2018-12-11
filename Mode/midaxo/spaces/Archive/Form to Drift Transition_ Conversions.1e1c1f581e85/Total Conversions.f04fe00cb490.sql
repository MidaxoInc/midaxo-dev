select
a.weeklabel,
a.monthlabel,
a.event_type,
count(concat(a.contact_id, a.contact_event_no)) as eventcount,
count(distinct a.company_id) as unique_company_eventcount
from midaxo.dev.event_timeline a
where a.year = '2018' and a.event_type in ('form','chat')
group by a.weeklabel, a.monthlabel, a.event_type
