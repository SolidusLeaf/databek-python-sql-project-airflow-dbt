{{ config(materialized='view') }}

-- Events that have a PAGER alert level. Rare, so this is a view over the
-- clean table instead of a stored copy.

select
    event_id,
    event_time,
    place,
    magnitude,
    alert,
    tsunami,
    significance,
    felt_reports,
    cdi,
    mmi,
    status,
    event_url
from {{ ref('earthquakes') }}
where alert is not null