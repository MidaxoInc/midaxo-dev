select
<<<<<<< HEAD
  hs.deal_id,
  hs.is_deleted,
  hs.deal_pipeline_stage_id,
  hs.deal_pipeline_id,
  hs.owner_id,
  hsc.company_id,
  hs.property_dealname,
  hs.property_closedate,
  hs.property_createdate,
  hs.property_amount,
  hs.property_attributed_to


from raw.hubspot.deal as hs
join raw.hubspot.deal_company as hsc on hs.deal_id = hsc.deal_id
=======
  d.deal_id,
  c.company_id,
  f.property_name as company_name,
  d.deal_pipeline_stage_id,
  d.deal_pipeline_id,
  d.owner_id,
  d.property_dealname as deal_name,
  d.property_closedate as closedate,
  d.property_createdate as createdate,
  d.property_amount as deal_amount,
  d.property_attributed_to as deal_attributed_to
from
  raw.hubspot.deal d
left join
  raw.hubspot.deal_company c
  on c.deal_id = d.deal_id
left join
  raw.hubspot.company f
  on f.id = c.company_id
order by closedate desc
>>>>>>> cf2d6a5f9d5c3c8f6d1f8b24516ba5d87c2c1e64
