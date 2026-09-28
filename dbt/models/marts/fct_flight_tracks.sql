-- Connects consecutive snapshots per aircraft into flight-path trajectories.
--
-- A gap of more than 10 minutes between two consecutive observations of the same
-- icao24 starts a new "session" (track) rather than being treated as one continuous
-- flight — otherwise an aircraft seen on two different days would get a single
-- flight path drawn straight across the days it was parked elsewhere.
with ordered as (

    select
        icao24,
        callsign,
        latitude,
        longitude,
        baro_altitude,
        velocity_knots,
        heading,
        vertical_rate,
        snapshot_ts,
        lag(snapshot_ts) over (partition by icao24 order by snapshot_ts) as prev_snapshot_ts
    from {{ ref('stg_states') }}

),

sessioned as (

    select
        *,
        sum(
            case
                when prev_snapshot_ts is null
                    or date_diff('second', prev_snapshot_ts, snapshot_ts) > 600
                then 1
                else 0
            end
        ) over (partition by icao24 order by snapshot_ts) as session_num
    from ordered

)

select
    icao24,
    any_value(callsign) as callsign,
    icao24 || '-' || session_num as track_id,
    min(snapshot_ts) as track_start_ts,
    max(snapshot_ts) as track_end_ts,
    count(*) as num_points,
    avg(velocity_knots) as avg_velocity_knots,
    min(baro_altitude) as min_altitude,
    max(baro_altitude) as max_altitude,
    -- DuckDB spatial's ST_Point() + ST_Distance_Sphere()/ST_MakeLine() here take
    -- (latitude, longitude) -- NOT the OGC (longitude, latitude) convention.
    -- Verified against known BWI<->DCA / IAD<->DCA distances; swapping this silently
    -- produces a still-plausible-looking but wrong geometry.
    ST_MakeLine(array_agg(ST_Point(latitude, longitude) order by snapshot_ts)) as flight_path
from sessioned
group by icao24, session_num
having count(*) > 1
