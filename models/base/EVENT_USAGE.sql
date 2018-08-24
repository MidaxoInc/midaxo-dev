select
a.time::timestamp_ntz as eventdate,
lower(a.name) as usage_event,
a.distinct_id as user_id,
a.properties:"Company ID"::string as company_id,
a.properties:"Process ID"::string as process_id,
lower(a.properties:"Process Level Role"::string) as user_role,
lower(a.properties:"Tab Name"::string) as navigation_tab,
lower(a.properties:"Sub-Nav Tab Name"::string) as navigation_subtab,
lower(a.properties:"User Role"::string) as adduser_role,
lower(a.properties) as event_properties
from raw.mixpanel.event a
