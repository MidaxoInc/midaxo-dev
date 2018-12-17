with qual AS
  (SELECT a.*,
          row_number() over (partition BY a.deal_id
                             ORDER BY a.dealcreatedate ASC) AS dupes
   FROM MIDAXO.DEV.pipecreated_attr a),
     won AS
  (SELECT a.*
   FROM MIDAXO.DEV.DEAL a
   WHERE a.pipeline_type = 'direct'
     AND a.pipeline_stage = 'closed won'
     and to_date(a.createdate) <> to_date(a.closedate)),
     datespine as
   (select d.ddate
   from midaxo.dev.datetable_clean d
    where d.ddate between dateadd('week',-52,last_day(current_date,'week')) and last_day(current_date,'week'))

select  d.ddate,
        q.dealcreatedate,
        q.deal_id,
        q.company_name,
        q.territory,
        q.icp_score,
        datediff('day',q.dealcreatedate,d.ddate) as days_to_win,
        case when w.recognized_arr is null then w.deal_amount else w.recognized_arr end as arr
from datespine d
left join won w
  on to_date(w.closedate) = d.ddate
left join qual q
  on q.deal_id = w.deal_id
  and q.dupes = 1
order by d.ddate desc