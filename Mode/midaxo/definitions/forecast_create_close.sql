with cc AS
    (SELECT *
   FROM {{ @forecast_create_plug }}),
     ef AS
  (SELECT *
   FROM {{ @forecast_existing_pipe }}),
    deal as
    (select distinct
      last_day(a.closedate,'quarter') as quarter,
      count(a.deal_id) over (partition by quarter) as dealcount
      from MIDAXO.DEV.DEAL a
      where a.pipeline_stage = 'closed won'
      and last_day(a.createdate,'quarter') = last_day(a.closedate,'quarter')
      and a.pipeline_type = 'direct')
SELECT c.ddate,
       c.forecast/(1-c.forecast)*e.forecast as plug,
       d.dealcount as actual,
       plug/d.dealcount - 1 as forecast_error
FROM cc c
left join ef e
  ON e.ddate = dateadd('day',5,last_day(dateadd('quarter',-1,c.ddate),'quarter'))
left join deal d
  on d.quarter = last_day(c.ddate, 'quarter')
where last_day(c.ddate,'quarter') <= last_day(current_date,'quarter')
order by c.ddate desc