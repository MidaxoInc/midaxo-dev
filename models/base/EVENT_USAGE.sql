select
a.time::timestamp_ntz as eventdate,
a.distinct_id as user_id,
a.name as usage_event,
a.properties:"Company ID"::string as company_id,
a.properties:"Process ID"::string as process_id,
a.properties:"Process Level Role"::string as user_role,
a.properties:"Tab Name"::string as navigation_tab,
a.properties:"Sub-Nav Tab Name"::string as navigation_subtab,
a.properties:"User Role"::string as adduser_role,
a.properties
from raw.mixpanel.event a
