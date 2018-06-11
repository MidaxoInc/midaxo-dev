--WIP, need to write logic to pull in verified ARR from contract else use amount from deal (make sure to pull the verified flag from the contract table)
select
a.deal_id,
a.company_id,
a.deal_pipeline_stage_id,
a.deal_pipeline_id,
a.owner_id,
a.property_dealname as deal_name,
a.property_name as company_name,
a.property_closedate as closedate,
a.pipeline_type,
a.pipeline_stage,
a.property_amount as deal_amount,
d.property_attributed_to as deal_attributed_to,
d.property_recognized_arr as recognized_arr,
d.property_one_time_revenue as one_time_revenue,
d.property_deal_contract_verified as finance_verified,
d.property_seats_purchased as seats_purchased,
d.property_contract_term_months as contract_term,
d.property_dealtype as deal_type
from {{ref('DEAL')}} a
left join {{ref('CONTRACT')}} b
  on case when a.deal_id = b.deal_id
where a.pipeline_stage = 'Closed Won'
