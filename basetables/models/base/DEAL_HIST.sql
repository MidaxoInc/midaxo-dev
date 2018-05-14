select
deal_id,
timestamp,
case when name = 'dealstage' then value else null end as dealstage,
case when name = 'amount' then value else null end as amount
from raw.hubspot.deal_history
