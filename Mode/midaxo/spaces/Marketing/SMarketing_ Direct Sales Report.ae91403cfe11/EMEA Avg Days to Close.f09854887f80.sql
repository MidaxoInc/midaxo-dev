with dealhist AS
    (SELECT distinct a.deal_id,
                     c.company_id,
                     CASE
                         WHEN ifnull(x.property_sales_territory,'na')='na' then 'na'
                         WHEN lower(x.property_sales_territory) = 'nam' then 'nam'
                         WHEN lower(x.property_sales_territory) in ('uk',
                                                                    'nordics',
                                                                    'benelux',
                                                                    'dach',
                                                                    'eur',
                                                                    'mea') then 'emea'
                         ELSE 'intl'
                     END AS region,
                     c.deal_name,
                     c.company_name,
                     c.deal_amount,
                     c.pipeline_stage AS current_stage,
                     to_date(a.timestamp) AS ddate,
                     lower(b.label) AS pipeline_stage,
                     row_number() over (partition BY a.deal_id, lower(b.label)
                                      ORDER BY a.timestamp ASC) AS dupe
   FROM RAW.HUBSPOT2.DEAL_PROPERTY_HISTORY a
   left join raw.hubspot2.deal_pipeline_stage b
     ON a.value = b.stage_id
   left join midaxo.dev.deal c
     ON c.deal_id = a.deal_id
   left join raw.hubspot2.company x
    on c.company_id = x.id
   WHERE a.name = 'deal_pipeline_stage_id'
     AND c.pipeline_type = 'direct'
     AND lower(b.label) in ('qualification',
                            'discovery',
                            'demo',
                            'evaluation',
                            'signatures',
                            'closed won',
                            'closed lost')
     and region = 'emea')
SELECT c.deal_id,
       c.ddate AS createdate,
       close.ddate AS ddate,
       c.deal_name,
       c.company_name,
       c.deal_amount,
       c.current_stage,
       datediff('day',c.ddate,ifnull(disco.ddate,close.ddate)) AS days_in_qual,
       datediff('day',disco.ddate,ifnull(demo.ddate,close.ddate)) AS days_in_disco,
       datediff('day',demo.ddate, ifnull(eval.ddate,close.ddate)) AS days_in_demo,
       datediff('day',eval.ddate,ifnull(sig.ddate,close.ddate)) AS days_in_eval,
       datediff('day',sig.ddate,close.ddate) AS days_in_sig,
       datediff('day',c.ddate,close.ddate) AS days_to_close
FROM dealhist c
left join dealhist disco
  ON c.deal_id = disco.deal_id
  AND disco.dupe = 1
  AND disco.pipeline_stage = 'discovery'
left join dealhist demo
  ON c.deal_id = demo.deal_id
  AND demo.dupe = 1
  AND demo.pipeline_stage = 'demo'
left join dealhist eval
  ON c.deal_id = eval.deal_id
  AND eval.dupe = 1
  AND eval.pipeline_stage = 'evaluation'
left join dealhist sig
  ON c.deal_id = sig.deal_id
  AND sig.dupe = 1
  AND sig.pipeline_stage = 'signatures'
left join dealhist close
  ON c.deal_id = close.deal_id
  AND close.dupe = 1
  AND close.pipeline_stage in ('closed won',
                               'closed lost')
WHERE c.dupe = 1
  AND c.pipeline_stage = 'qualification'
  AND c.ddate < close.ddate
  AND datediff('day',c.ddate,ifnull(disco.ddate,close.ddate))>0