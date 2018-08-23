with
  event as (
    select
      a.eventdate,
      a.company_id,
      a.contact_id,
      a.event_type,
      a.event_action,
      a.event_source,
      case
        when a.event_type in ('form','chat') then 'inbound'
        when a.event_owner_campaign_url ilike '%demo follow-up%' then 'inbound'
        when a.event_owner_campaign_url ilike '%webinar%' then 'inbound'
        when a.event_type in ('sales_email','sales_call','meeting') then 'outbound'
        else 'other'
      end as event_category,
      a.event_owner_campaign_url,
      a.company_event_no,
      a.contact_event_no
    from {{ref('EVENT_TIMELINE')}} a
    ),
    firstevent as (
      select
      a.eventdate,
      a.company_id,
      case
        when a.event_type in ('form','chat') then 'inbound'
        when a.event_owner_campaign_url ilike '%demo follow-up%' then 'inbound'
        when a.event_owner_campaign_url ilike '%webinar%' then 'inbound'
        when a.event_type in ('sales_email','sales_call','meeting') then 'outbound'
        else 'other'
      end as first_event_category
      from {{ref('EVENT_TIMELINE')}} a
      where a.company_event_no = 1
    ),
  pipecreated as (
    select
      to_date(b.validfrom) as ddate,
      b.company_id,
      b.deal_id,
      b.deal_name,
      b.pipeline_type,
      b.pipeline_stage,
      row_number() over (partition by b.deal_id order by b.validfrom asc) as x
    from {{ref('DEAL_ARCHIVE_CLEAN')}}  b
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
     p.deal_id,
     p.deal_name
   from event e
   inner join pipecreated p
    on
      p.company_id = e.company_id
      and p.x=1
      and e.eventdate <= p.ddate
    where e.event_category <> 'other'
  ),

  asp as (
    select distinct
      d.ddate,
      avg(a.deal_amount) over (partition by d.ddate) as t180_asp
    from MIDAXO.DEV.datetable_clean d
    left join MIDAXO.DEV.deal a
      on a.closedate between dateadd('day',-180, d.ddate) and d.ddate
    where
      contains(a.pipeline_stage, 'won') = true
      and a.pipeline_type = 'direct'
  ),

  company as (
    select
      c.id as company_id,
      c.property_name as company_name,
      c.property_country as country,
      c.property_sales_territory as territory,
      c.property_icp_score as icp_score
    from raw.hubspot.company c
  )

select distinct
  t.dealcreatedate,
  t.firsttouchdate,
  t.company_id,
  t.deal_id,
  c.company_name,
  t.deal_name,
  c.territory,
  c.country,
  c.icp_score,
  f.first_event_category,
  t.event_category,
  t.event_type,
  t.event_action,
  t.event_source,
  t.event_owner_campaign_url,
  a.t180_asp,
  count(*) over (partition by t.deal_id) as total_eventcount,
  count(*) over (partition by t.deal_id,t.event_category,t.event_type,t.event_action,t.event_source,t.event_owner_campaign_url) as detail_eventcount,
  count(*) over (partition by t.deal_id,t.event_category,t.event_type,t.event_action,t.event_source,t.event_owner_campaign_url)/count(*) over (partition by t.deal_id) as detail_share,
  detail_share * a.t180_asp as attributed_pipeline_created
from attribution t
left join asp a
  on a.ddate = t.dealcreatedate
left join company c
  on c.company_id = t.company_id
left join firstevent f
  on f.company_id = t.company_id
order by t.dealcreatedate, t.deal_id
