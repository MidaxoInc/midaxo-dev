select
  last_day(to_date(a.month),'month') as ddate,
  a.user_email as email,
  b.owner_id,
  b.first_name || ' ' || b.last_name as rep,
  a.role,
  a.team,
  a.geo,
  c.first_name || ' ' || c.last_name as manager,
  a.territory,
  to_date(a.hire_date) as hiredate,
  to_date(a.in_funnel_month) as infunneldate,
  a.arr_quota,
  a.meetings_quota
from raw.manual.mstd a
left join raw.hubspot.owner b
  on a.user_email = b.email
left join raw.hubspot.owner c
  on a.manager_email = c.email
