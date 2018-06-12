--manual input from google sheet: https://docs.google.com/spreadsheets/d/184huuNfROb_lbOwxJ0IOBxpVrll6QKG8lxAkZlfYlgE/edit#gid=0
select
a.deal_id,
a.company_name,
a.deal_name,
a.pipeline_type,
a.pipeline_stage,
a.deal_type,
a.closedate::timestamp_tz as closedate,
a.deal_amount::float as deal_amount,
a.seats_purchased::float as seats_purchased,
a.recognized_arr::float as recognized_arr,
a.one_time_revenue::float as one_time_revenue,
a.contract_startdate::timestamp_tz as contract_startdate,
a.contract_term::float as contract_term,
a.contract_renewaldate::timestamp_tz as contract_renewaldate,
a.finance_verified
from raw.manual.contracts a
where a.finance_verified = 'TRUE'
