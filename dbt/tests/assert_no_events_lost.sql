-- Fails if an event exists in raw but is missing from the clean table.
select distinct r.event_id
from {{ source('raw', 'raw_events') }} as r
left join {{ ref('earthquakes') }} as e on r.event_id = e.event_id
where e.event_id is null