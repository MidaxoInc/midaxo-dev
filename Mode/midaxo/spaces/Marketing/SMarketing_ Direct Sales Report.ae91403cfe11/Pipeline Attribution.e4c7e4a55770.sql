 SELECT distinct a.dealcreatedate as ddate,
        a.company_id,
        a.deal_id,
        a.company_name,
        lower(a.territory) as territory,
        a.icp_score,
        case when a.event_source = 'website' then 'organic' else a.event_source end as event_source,
        a.event_type,
        a.event_category,
        sum(a.detail_share) over (partition by a.deal_id, a.event_source, a.event_type) as share,
        sum(a.detail_share) over (partition by a.deal_id, a.event_source, a.event_type) * a.deal_value as attr_arr
   FROM midaxo.dev.pipecreated_attr a
   WHERE last_day(a.dealcreatedate,'week') between dateadd('week',-52,last_day(current_date,'week')) AND last_day(current_date,'week')
   order by a.deal_id, a.dealcreatedate desc