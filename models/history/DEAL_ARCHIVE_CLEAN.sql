-- 7/1 - need to add logic to use 'partner engagement' pipeline prior to 7/1, and 'engagement' onwards
-- 8/15 - need to add unique row identifiers and remove duplicates
select
  a.deal_id,
  d.deal_name,
  case  when a.company_id is null then d.company_id
        else a.company_id end as company_id,
  d.company_name,
  a.deal_pipeline_stage_id,
  a.deal_pipeline_id,
  a.forecast_stage,
  a.deal_type,
  p.pipeline_type,
  p.pipeline_stage,
  p.pipeline_stageorder,
  a.owner_id,
  a.closedate,
  a.createdate,
  a.deal_attributed_to,
  a.deal_amount,
  a."valid_from" as validfrom,
  case when a."valid_to" is NULL then least(current_date(), a.closedate) else a."valid_to" end as validto
from midaxo.dev.deal_archive a
left join {{ref('PIPELINE_PROPERTY')}} p
  on p.stage_id = a.deal_pipeline_stage_id
left join {{ref('DEAL')}} d
  on d.deal_id = a.deal_id
order by deal_id, closedate desc
