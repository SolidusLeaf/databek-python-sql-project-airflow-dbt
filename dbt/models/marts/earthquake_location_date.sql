{{ config(materialized='table') }}

-- Where and when: one row per earthquake with place, date and time pieces,
-- ready for grouping on a dashboard. Keeps all event types; filter on
-- event_type = 'earthquake' downstream if you only want earthquakes.

with base as (

    select
        *,
        -- "14 km WSW of Conconully, Washington" -> "Conconully, Washington"
        regexp_replace(place, '^[0-9]+ km [NSEW]+ of ', '') as nearest_place
    from {{ ref('earthquakes') }}

)

select
    event_id,
    event_time,
    event_date,
    cast(extract(hour from event_time) as integer)              as event_hour,
    place,
    nearest_place,
    -- last comma-separated part: state, country or region. Naming is not
    -- uniform in USGS data (for example "CA" next to "California").
    trim(string_split(nearest_place, ',')[-1])                  as area,
    network,
    latitude,
    longitude,
    depth_km,
    magnitude,
    case
        when magnitude is null then 'unknown'
        when magnitude < 2     then 'below 2'
        when magnitude < 4     then '2 to 3.9'
        when magnitude < 6     then '4 to 5.9'
        else '6 and above'
    end                                                         as magnitude_bucket,
    event_type,
    status
from base