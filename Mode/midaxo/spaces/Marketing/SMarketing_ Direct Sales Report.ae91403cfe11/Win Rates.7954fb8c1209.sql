select  last_day(d.closedate,'month') as ddate,
        d.deal_id,
        d.company_id,
        m.rep as owner,
        d.company_name,
        d.territory,
        c.property_icp_score as icp_score,
        d.pipeline_stage,
        sum(case when d.pipeline_stage = 'closed won' then 1 else 0 end) over (partition by d.deal_id, last_day(d.closedate,'month')) as dealwon,
        count(*) over (partition by last_day(d.closedate,'month')) as totalclose,
        sum(case when d.pipeline_stage = 'closed won' then 1 else 0 end) over (partition by d.deal_id, last_day(d.closedate,'month'))
          / count(*) over (partition by last_day(d.closedate,'month')) as win_rate
from midaxo.dev.deal d
left join raw.hubspot2.company c
  on c.id = d.company_id
left join midaxo.dev.mstd m
  on d.owner_id = m.owner_id
  and last_day(d.closedate,'month') = last_day(m.ddate,'month')
where d.pipeline_type = 'direct'
and d.pipeline_stage in ('closed lost','closed won')
and date_part('year',d.closedate) > 2015
and d.closedate < current_date
