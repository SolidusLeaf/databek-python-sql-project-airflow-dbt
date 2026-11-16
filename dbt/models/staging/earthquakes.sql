{{ config(materialized='table') }}

-- One row per earthquake: the LATEST version of each event in raw.raw_events,
-- with typed columns pulled out of the stored JSON.

with versions as (

    select
        *,
        row_number() over (partition by event_id order by updated_ms desc) as version_rank,
        count(*)     over (partition by event_id)                          as versions_seen
    from {{ source('raw', 'raw_events') }}

),

latest as (

    select * from versions where version_rank = 1

)

select
    event_id,
    payload ->> '$.properties.net'                                           as network,
    round(try_cast(payload ->> '$.properties.mag' as double), 2)             as magnitude,
    payload ->> '$.properties.magType'                                       as magnitude_type,
    payload ->> '$.properties.place'                                         as place,

    -- USGS gives epoch milliseconds; epoch_ms() turns them into UTC timestamps
    epoch_ms(try_cast(payload ->> '$.properties.time' as bigint))            as event_time,
    cast(epoch_ms(try_cast(payload ->> '$.properties.time' as bigint)) as date) as event_date,
    epoch_ms(updated_ms)                                                     as updated_at,

    -- coordinates are [longitude, latitude, depth_km]; round away float noise
    round(try_cast(payload ->> '$.geometry.coordinates[1]' as double), 4)    as latitude,
    round(try_cast(payload ->> '$.geometry.coordinates[0]' as double), 4)    as longitude,
    round(try_cast(payload ->> '$.geometry.coordinates[2]' as double), 2)    as depth_km,

    payload ->> '$.properties.status'                                        as status,
    payload ->> '$.properties.type'                                          as event_type,
    try_cast(payload ->> '$.properties.tsunami' as integer)                  as tsunami,
    try_cast(payload ->> '$.properties.sig' as integer)                      as significance,
    payload ->> '$.properties.alert'                                         as alert,
    try_cast(payload ->> '$.properties.felt' as integer)                     as felt_reports,
    try_cast(payload ->> '$.properties.cdi' as double)                       as cdi,
    try_cast(payload ->> '$.properties.mmi' as double)                       as mmi,
    try_cast(payload ->> '$.properties.nst' as integer)                      as stations_used,
    payload ->> '$.properties.url'                                           as event_url,

    versions_seen,
    ingested_at
from latest