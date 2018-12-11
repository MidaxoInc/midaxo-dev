select  d.closedate as ddate, 
        d.createdate,
        d.deal_id,
        d.company_id,
        d.company_name,
        c.property_sales_territory as territory,
        c.property_icp_score as icp_score,
        ifnull(d.recognized_arr,d.deal_amount) as recognized_arr
from midaxo.dev.deal d
left join raw.hubspot2.company c
  on c.id = d.company_id
where d.pipeline_type = 'direct'
  and d.pipeline_stage = 'closed won'
  and ifnull(d.is_partner,false) = false
order by d.closedate desc