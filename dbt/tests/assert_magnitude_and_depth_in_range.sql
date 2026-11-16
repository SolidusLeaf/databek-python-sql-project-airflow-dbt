-- Fails if a magnitude or depth is physically implausible.
-- A NULL magnitude is allowed (USGS sometimes has none).
select event_id, magnitude, depth_km
from {{ ref('earthquakes') }}
where (magnitude is not null and magnitude not between -2 and 10)
   or (depth_km  is not null and depth_km  not between -10 and 800)