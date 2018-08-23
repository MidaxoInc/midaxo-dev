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
  pipewon as (
    select
      b.closedate as ddate,
      b.company_id,
      b.deal_id,
      b.deal_name,
      b.pipeline_type,
      b.pipeline_stage,
      b.recognized_arr
    from {{ref('ARR')}}  b
    where
      b.pipeline_type = 'direct'
      and b.pipeline_stage = 'closed won'
      ),

  attribution as (
   select
     e.*,
     p.ddate as dealclosedate,
     min(e.eventdate) over (partition by p.deal_id) as firsttouchdate,
     p.deal_id,
     p.deal_name,
     p.recognized_arr
   from event e
   inner join pipewon p
    on
      p.company_id = e.company_id
      and e.eventdate <= p.ddate
    where e.event_category <> 'other'
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
  t.dealclosedate,
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
  t.recognized_arr,
  count(*) over (partition by t.deal_id) as total_eventcount,
  count(*) over (partition by t.deal_id,t.event_category,f.first_event_category,t.event_type,t.event_action,t.event_source,t.event_owner_campaign_url) as detail_eventcount,
  count(*) over (partition by t.deal_id,t.event_category,t.event_type,t.event_action,t.event_source,t.event_owner_campaign_url)/count(*) over (partition by t.deal_id) as detail_share,
  detail_share * t.recognized_arr as attributed_pipeline_won
from attribution t
left join company c
  on c.company_id = t.company_id
left join firstevent f
  on f.company_id = t.company_id
order by t.dealclosedate, t.deal_id
