-- Fails if the model kept an OLDER version of an event than the newest in raw.
select e.event_id, e.updated_at, epoch_ms(r.latest_ms) as latest_in_raw
from {{ ref('earthquakes') }} as e
join (
    select event_id, max(updated_ms) as latest_ms
    from {{ source('raw', 'raw_events') }}
    group by event_id
) as r on e.event_id = r.event_id
where e.updated_at <> epoch_ms(r.latest_ms)