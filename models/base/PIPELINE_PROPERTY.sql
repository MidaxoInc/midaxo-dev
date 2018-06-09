select
a.stage_id,
a.label as pipeline_stage,
b.pipeline_id,
b.label as pipeline_type
from raw.hubspot.deal_pipeline_stage a
left join raw.hubspot.deal_pipeline b
  on b.pipeline_id = a.pipeline_id
