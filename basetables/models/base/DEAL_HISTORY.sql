select
  h.deal_id,
  h.timestamp as changedate,
  h.value::varchar as value,
  h.name as dealproperty
  from raw.hubspot.deal_history h
union all
select
  a.deal_id,
  a.timestamp,
  a.amount::varchar,
  'amount'
from raw.hubspot.deal_amount_history a
order by deal_id, value, changedate desc
