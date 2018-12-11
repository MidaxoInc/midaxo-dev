select
  'Manager ML' as forecast,
  a.deal_name,
  a.closedate,
  a.pipeline_stage,
  a.forecast_stage,
  a.deal_amount as arr
from midaxo.dev.deal a
where a.pipeline_type = 'direct'
and last_day(a.closedate, 'quarter') = last_day(current_date,'quarter')
and a.forecast_stage in ('most likely','commit','won')