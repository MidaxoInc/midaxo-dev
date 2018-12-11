with companies as
(select distinct  c.id as company_id,
                  c.property_sales_territory as territory,
                  ifnull(c.property_icp_score,'Non-ICP') as icp_score
 from raw.hubspot2.company c
 where c.property_createdate < dateadd('week',-1,last_day(current_date,'week'))),

engaged AS
(SELECT distinct e.company_id
FROM midaxo.dev.event_timeline e
left join raw.hubspot2.company c
  on c.id = e.company_id
WHERE e.eventdate between dateadd('week',-13,last_day(current_date,'week')) AND dateadd('week',-1,last_day(current_date,'week'))),

activedeal as
  (SELECT distinct a.company_id
   FROM MIDAXO.DEV.DEAL_ARCHIVE_CLEAN a
   WHERE a.pipeline_stage not in ('closed lost',
                                  'closed won',
                                  'activation',
                                  'nurture')
     AND a.pipeline_type in ('direct')
     AND a.validfrom between dateadd('week',-13,last_day(current_date,'week')) AND dateadd('week',-1,last_day(current_date,'week'))),

qualified as
  (SELECT distinct a.company_id
   FROM MIDAXO.DEV.DEAL_ARCHIVE_CLEAN a
   WHERE a.pipeline_stage in ('discovery','demo','evaluation','signatures')
     AND a.pipeline_type in ('direct')
     AND a.validfrom between dateadd('week',-13,last_day(current_date,'week')) AND dateadd('week',-1,last_day(current_date,'week'))),

won as
  (SELECT distinct a.company_id, a.company_name
   FROM MIDAXO.DEV.DEAL_ARCHIVE_CLEAN a
   WHERE a.pipeline_stage in ('closed won')
     AND a.pipeline_type in ('direct')
     AND a.closedate between dateadd('week',-13,last_day(current_date,'week')) AND dateadd('week',-1,last_day(current_date,'week'))),

lost as
  (SELECT distinct a.company_id
   FROM MIDAXO.DEV.DEAL_ARCHIVE_CLEAN a
   WHERE a.pipeline_stage in ('closed lost')
     AND a.pipeline_type in ('direct')
     AND a.closedate between dateadd('week',-13,last_day(current_date,'week')) AND dateadd('week',-1,last_day(current_date,'week'))),

totals as
(select distinct last_day(current_date,'week') as ddate,
                t.territory as territory,
                t.icp_score as icp_score,
                count(t.company_id) over (partition by ddate, t.territory, t.icp_score) as total,
                count(e.company_id) over (partition by ddate, t.territory, t.icp_score) as engaged,
                count(a.company_id) over (partition by ddate, t.territory, t.icp_score) as active_deal,
                count(q.company_id) over (partition by ddate, t.territory, t.icp_score) as qualified_deal,
                count(w.company_id) over (partition by ddate, t.territory, t.icp_score) as won
from companies t
left join engaged e
on e.company_id = t.company_id
left join activedeal a
on a.company_id = t.company_id
left join qualified q
on q.company_id = t.company_id
left join won w
on w.company_id = t.company_id
left join lost l
on l.company_id = t.company_id),
pivot as (
select  t.*
from totals t
unpivot (companies for measure in (total,engaged,active_deal,qualified_deal,won)))
select  p.*,
        case  when lower(p.measure) = 'total' then 1
              when lower(p.measure) = 'engaged' then 2
              when lower(p.measure) = 'active_deal' then 3
              when lower(p.measure) = 'qualified_deal' then 4
              when lower(p.measure) = 'won' then 5 end as sort_order
from pivot p
order by sort_order asc