-- Fails (returns rows) if any event has an impossible latitude or longitude.
select event_id, latitude, longitude
from {{ ref('earthquakes') }}
where latitude is null
   or longitude is null
   or latitude  not between -90  and 90
   or longitude not between -180 and 180