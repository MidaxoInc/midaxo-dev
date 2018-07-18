select
a.stage_id,
lower(a.label) as pipeline_stage,
a.display_order as pipeline_stageorder,
b.pipeline_id,
lower(b.label) as pipeline_type
from raw.hubspot.deal_pipeline_stage a
left join raw.hubspot.deal_pipeline b
  on b.pipeline_id = a.pipeline_id
