WITH datespine AS
  (SELECT a.ddate,
          last_day(a.ddate,'quarter') AS quarter,
          to_number(extract('quarter', a.ddate)) AS qr,
          to_number(extract('month', a.ddate)) AS mo,
          (CASE
               WHEN qr = 1 THEN mo
               WHEN qr = 2 THEN mo - 3
               WHEN qr = 3 THEN mo - 6
               WHEN qr = 4 THEN mo - 9
           END) AS quartermonth
   FROM midaxo.dev.datetable_clean a
   WHERE a.ddate between dateadd('quarter',-10,dateadd('day',1,last_day(current_date,'quarter'))) AND last_day(dateadd('year',1,current_date),'quarter')),
     currentdeal AS
  (SELECT a.deal_id,
          a.closedate,
          a.pipeline_stage
   FROM midaxo.dev.deal a
   WHERE a.pipeline_type = 'direct'
    and a.pipeline_stage not in ('activation','nurture')
    and ifnull(a.is_partner,false) = false),
     dedupe AS
  (SELECT DISTINCT last_day(d.ddate,'month') AS mo,
                   d.quartermonth,
                   a.deal_id,
                   a.pipeline_stage,
                   CASE
                       WHEN last_day(c.closedate,'quarter') <> last_day(a.closedate,'quarter') then 'pushed'
                       WHEN c.pipeline_stage = 'closed won' then 'won'
                       WHEN c.pipeline_stage = 'closed lost' then 'lost'
                       ELSE 'open'
                   END AS status
   FROM datespine d
   left join MIDAXO.DEV.DEAL_ARCHIVE_CLEAN a
     ON d.ddate between a.validfrom AND a.validto
   left join currentdeal c
     ON c.deal_id = a.deal_id
     WHERE a.pipeline_type = 'direct'
       AND status <> 'open'
       AND a.pipeline_stage not in ('activation',
                                    'nurture',
                                    'qualification',
                                    'closed won',
                                    'closed lost')
       AND last_day(a.closedate, 'quarter') = last_day(d.ddate,'quarter')
       AND last_day(a.createdate,'quarter') < last_day(d.ddate,'quarter')),
     dealstatus AS
    (SELECT d.mo,
            d.quartermonth,
            d.pipeline_stage,
            sum(CASE
                    WHEN d.status = 'won' then 1
                    ELSE 0
                END) AS dwin,
            sum(CASE
                    WHEN d.status = 'pushed' then 1
                    ELSE 0
                END) AS dpush,
            sum(CASE
                    WHEN d.status = 'lost' then 1
                    ELSE 0
                END) AS dlost,
            count(deal_id) AS dtotal
   FROM dedupe d
   GROUP BY mo,
            d.quartermonth,
            d.pipeline_stage)
SELECT distinct d.ddate,
       s.pipeline_stage,
       sum(s.dwin) over (partition by d.ddate, s.pipeline_stage) as win,
       sum(s.dpush) over (partition by d.ddate, s.pipeline_stage) as push,
       sum(s.dlost) over (partition by d.ddate, s.pipeline_stage) as lost,
       sum(s.dtotal) over (partition by d.ddate, s.pipeline_stage) as total,
       win/total AS real_win_rate,
       push/total AS push_rate
FROM datespine d
left join dealstatus s
  ON s.quartermonth = d.quartermonth
  AND s.mo between dateadd('quarter',-4,last_day(d.ddate,'quarter')) AND dateadd('quarter',-1,last_day(d.ddate,'quarter'))
WHERE d.ddate between dateadd('quarter',-5,dateadd('day',1,last_day(current_date,'quarter'))) AND last_day(dateadd('year',1,current_date),'quarter')
order by ddate desc