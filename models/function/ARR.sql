--WIP, need to write logic to pull in verified ARR from contract else use amount from deal (make sure to pull the verified flag from the contract table)
select
c.*,
a.deal_id,
a.company_id,
a.deal_pipeline_stage_id,
a.deal_pipeline_id,
a.owner_id,
a.deal_name,
a.company_name,
a.closedate,
a.pipeline_type,
a.pipeline_stage,
a.deal_type,
a.deal_attributed_to,
--logic to pull verified arr info from deal, then contract, then unverified based on 'verified' flag
case
  when a.finance_verified = 'TRUE'
  then a.recognized_arr
  else
    case
      when b.finance_verified = 'TRUE'
      then b.recognized_arr
      else a.deal_amount
      end
end as recognized_arr,
--same for one time revenue
case
  when a.finance_verified = 'TRUE'
  then a.one_time_revenue
  else
    case
      when b.finance_verified = 'TRUE'
      then b.one_time_revenue
      else null
      end
end as one_time_revenue,
--same for seats purchased
case
  when a.finance_verified = 'TRUE'
  then a.seats_purchased
  else
    case
      when b.finance_verified = 'TRUE'
      then b.seats_purchased
      else null
      end
end as seats_purchased,
--same for contract term
case
  when a.finance_verified = 'TRUE'
  then a.contract_term
  else
    case
      when b.finance_verified = 'TRUE'
      then b.contract_term
      else null
      end
end as contract_term,
--same for verified flag
case
  when b.finance_verified = TRUE
  then b.finance_verified
  else a.finance_verified
  end as finance_verified

from {{ref('DEAL')}} a
left join {{ref('CONTRACT')}} b
  on a.deal_id = b.deal_id

left join {{ref('DATETABLE_CLEAN')}} c
  on to_date(a.closedate) = c.ddate

where a.pipeline_stage = 'Closed Won'
