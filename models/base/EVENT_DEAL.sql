-- need to update deal creation for direct as 'entered qualification' or 'created and not in closed lost, activation, or nurture'

with
  dealcreate as (
    select
      md5(a.deal_id||a.createdate) as event_id,
      a.deal_id::varchar as deal_id,
      null as contact_id,
      a.company_id as company_id,
      a.createdate::timestamp_ntz as eventdate,
      'deal_created' as eventtype,
      'na' as eventaction,
      'sales' as eventsource,
      a.owner_id::varchar as event_owner_campaign_url
    from {{ref('DEAL')}} a
    where a.company_id is not null
  ),
  dealclose as(
    select
       md5(b.deal_id||b.closedate) as event_id,
       b.deal_id::varchar as deal_id,
       null as contact_id,
       b.company_id as company_id,
       b.closedate::timestamp_ntz as eventdate,
       'deal_closed' as event_type,
       case
         when contains(lower(b.pipeline_stage), 'won') then 'won'
         else 'lost'
       end as event_action,
       'sales' as event_source,
       b.owner_id::varchar as event_owner_campaign_url
     from {{ref('DEAL')}} b
     where contains(lower(b.pipeline_stage), 'closed')
       and b.company_id is not null
       and to_date(b.closedate) <= current_date
  )

select * from dealcreate
union
select * from dealclose
