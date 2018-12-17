select distinct
e.date as ddate,
sum(e.responses) over (partition by e.date) / e.employees as response_rate,
sum(case when e.score >=9 then e.share
    when e.score <=6 then e.share * -1
    else 0 end) over (partition by e.date) * 100 as enps
from raw.manual.enps e