select
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
