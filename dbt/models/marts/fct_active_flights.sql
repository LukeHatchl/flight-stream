-- Current snapshot: everything airborne in the bbox as of the most recent poll.
with latest_snapshot as (

    select max(snapshot_ts) as snapshot_ts
    from {{ ref('stg_states') }}

)

select
    s.icao24,
    s.callsign,
    s.origin_country,
    s.latitude,
    s.longitude,
    s.baro_altitude,
    s.geo_altitude,
    s.velocity_ms,
    s.velocity_knots,
    s.heading,
    s.vertical_rate,
    s.squawk,
    s.snapshot_ts
from {{ ref('stg_states') }} as s
inner join latest_snapshot using (snapshot_ts)
