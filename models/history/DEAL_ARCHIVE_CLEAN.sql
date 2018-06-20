select
  a.deal_id,
  a.company_id,
  a.deal_pipeline_stage_id,
  a.deal_pipeline_id,
  a.owner_id,
  a.closedate,
  a.createdate,
  a.deal_attributed_to,
  a.deal_amount,
  a."valid_from" as validfrom,
  case when a."valid_to" is NULL then least(current_date(), a.closedate) else a."valid_to" end as validto
from midaxo.dev.deal_archive a
order by deal_id, closedate desc
