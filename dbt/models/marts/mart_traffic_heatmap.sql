-- Position density grid over the bbox — busiest corridors, across all collected history.
-- Grid cells are ~0.05 degrees (~5.5km at this latitude) on a side.
select
    floor(latitude / 0.05) * 0.05 as lat_bucket,
    floor(longitude / 0.05) * 0.05 as lon_bucket,
    count(*) as observation_count,
    count(distinct icao24) as distinct_aircraft_count,
    avg(baro_altitude) as avg_altitude,
    avg(velocity_knots) as avg_velocity_knots
from {{ ref('stg_states') }}
group by 1, 2
