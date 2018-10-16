select
c.*,
a.deal_id,
a.company_id,
a.deal_pipeline_stage_id,
a.deal_pipeline_id,
a.owner_id,
a.deal_name,
a.company_name,
a.createdate,
a.closedate,
case
  when (a.deal_type = 'new' and is_partner = 'TRUE')
  then 'partner new'
  else a.pipeline_type
end as pipeline_type,
a.pipeline_stage,
a.pipeline_stageorder,
a.deal_type,
a.deal_attributed_to,
a.engagement_partner,
a.recognized_arr,
b.one_time_revenue,
a.seats_purchased,
a.contract_term,
a.finance_verified


from {{ref('DEAL')}} a

left join {{ref('DATETABLE_CLEAN')}} c
  on to_date(a.closedate) = c.ddate

where a.pipeline_stage in ('closed won', 'renewed', 'churned')
