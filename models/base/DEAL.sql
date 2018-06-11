select
  d.deal_id,
  c.company_id,
  d.deal_pipeline_stage_id,
  d.deal_pipeline_id,
  d.owner_id,
  d.property_dealname as deal_name,
  f.property_name as company_name,
  d.property_closedate as closedate,
  d.property_createdate as createdate,
  d.property_hs_lastmodifieddate as changedate,
  p.pipeline_type,
  p.pipeline_stage,
  d.property_amount as deal_amount,
  d.property_attributed_to as deal_attributed_to,
  d.property_recognized_arr as recognized_arr,
  d.property_one_time_revenue as one_time_revenue,
  d.property_deal_contract_verified as finance_verified,
  d.property_seats_purchased as seats_purchased,
  d.property_contract_term_months as contract_term,
  d.property_dealtype as deal_type
from
  raw.hubspot.deal d
left join
  raw.hubspot.deal_company c
  on c.deal_id = d.deal_id
left join
  raw.hubspot.company f
  on f.id = c.company_id
left join
  {{ref('PIPELINE_PROPERTY')}} p
  on p.stage_id = d.deal_pipeline_stage_id

order by closedate desc
