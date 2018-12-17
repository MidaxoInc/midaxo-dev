with 
   firstconvert as (
    select 
      a.eventdate,
      a.company_id,
      a.contact_id,
      a.event_type,
      a.event_action,
      a.event_source,
      a.event_owner_campaign_url,
      case
        when a.event_type in ('form','chat') then 'inbound'
        when a.event_owner_campaign_url ilike '%demo follow-up%' then 'inbound'
        when a.event_owner_campaign_url ilike '%webinar%' then 'inbound'
        when a.event_type in ('sales_email','sales_call','meeting') then 'outbound'
        else 'other'
      end as event_category,
      a.company_event_no
    from MIDAXO.DEV.EVENT_TIMELINE a
    where a.company_event_no = 1
    ),
  event as (
    select 
      a.eventdate,
      a.company_id,
      a.contact_id,
      a.event_type,
      a.event_action,
      a.event_source,
      f.event_category as firstconvert,
      case
        when a.event_type in ('form','chat') then 'inbound'
        when a.event_type in ('sales_email','sales_call','meeting') then 'outbound'
        else firstconvert
      end as event_category,
      a.company_event_no
    from MIDAXO.DEV.EVENT_TIMELINE a
    left join firstconvert f
      on f.company_id = a.company_id
    ),
    
  pipecreated as (
    select
      to_date(b.validfrom) as ddate,
      b.company_id,
      b.deal_id,
      b.pipeline_type,
      b.pipeline_stage,
      row_number() over (partition by b.deal_id order by b.validfrom asc) as x
    from MIDAXO.DEV.DEAL_ARCHIVE_CLEAN  b
    where 
      b.pipeline_type = 'direct'
      and (b.pipeline_stage = 'qualification' 
        or (to_date(b.createdate) = ddate and b.pipeline_stage not in ('activation','nurture'))
      )
  ),
  
  attribution as (
   select 
     e.*,
     p.ddate as dealcreatedate,
     min(e.eventdate) over (partition by p.deal_id) as firsttouchdate,
     p.deal_id
   from event e
   inner join pipecreated p
    on 
      p.company_id = e.company_id
      and e.eventdate <= p.ddate
      and p.x=1
    where e.event_category <> 'other'
  ),
  
  asp as (
    select
      d.ddate,
      avg(a.deal_amount) as t180_asp
    from MIDAXO.DEV.datetable_clean d
    left join MIDAXO.DEV.deal a
      on a.closedate between dateadd('day',-180, d.ddate) and d.ddate
    where 
      contains(a.pipeline_stage, 'won') = true
      and a.pipeline_type = 'direct'
    group by d.ddate
  ),
  
  company as (
    select
      c.id as company_id,
      c.property_country as country,
      c.property_is_icp as icp,
      c.property_icp_score as icp_score
    from raw.hubspot.company c
  )  
  
select distinct
  t.dealcreatedate,
  t.firsttouchdate,
  t.company_id,
  c.icp_score,
  c.country,
  t.deal_id,
  t.event_category,
  t.event_type,
  t.event_action,
  t.event_source,
  a.t180_asp,
  case 
    when  icp_score is null then 'non-icp'
    else icp_score
  end as icp,
  count(*) over (partition by t.deal_id) as total_eventcount,
  count(*) over (partition by t.deal_id,t.event_category,t.event_type,t.event_action,t.event_source) as detail_eventcount,
  detail_eventcount/total_eventcount as detail_share,
  detail_share * a.t180_asp as detail_attr
from attribution t
left join asp a
  on a.ddate = t.dealcreatedate
left join company c
  on c.company_id = t.company_id
where last_day(t.dealcreatedate,'month') > dateadd('month',-15,last_day(current_date,'month'))
order by t.dealcreatedate, t.deal_id