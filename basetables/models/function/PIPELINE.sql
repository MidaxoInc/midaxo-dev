--WIP
--Table that shows daily deal value and stage from creation to close.

--select only dates at the "day" level
select
  days.ddate,
  deals.*,
  history.*
  from
-- select only unique days from the date table
    (select a.ddate
    from {{ref('DATETABLE')}} a
    order by ddate desc
    ) days
-- join deal property info to new days table if the deal was open
  left join
    (select *
    from {{ref('DEAL')}} as b
    order by b.closedate desc
    ) deals
  on
    (deals.closedate >= days.ddate
    and deals.createdate <=days.ddate
    )
