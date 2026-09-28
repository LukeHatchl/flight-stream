-- Geospatial join: every currently-airborne aircraft within {{ var('airport_proximity_km') }}km
-- of a US airport, with a phase inferred from altitude + vertical rate.
--
-- DuckDB spatial's ST_Point() + ST_Distance_Sphere() here take (latitude, longitude) --
-- NOT the OGC (longitude, latitude) convention. Verified against known BWI<->DCA /
-- IAD<->DCA distances; swapping this silently produces a still-plausible-looking but
-- wrong distance.
with joined as (

    select
        f.icao24,
        f.callsign,
        f.latitude as flight_latitude,
        f.longitude as flight_longitude,
        f.baro_altitude,
        f.velocity_knots,
        f.heading,
        f.vertical_rate,
        f.snapshot_ts,
        a.icao_code as airport_icao_code,
        a.name as airport_name,
        a.type as airport_type,
        a.latitude as airport_latitude,
        a.longitude as airport_longitude,
        ST_Distance_Sphere(
            ST_Point(f.latitude, f.longitude),
            ST_Point(a.latitude, a.longitude)
        ) / 1000.0 as distance_km
    from {{ ref('fct_active_flights') }} as f
    cross join {{ ref('stg_airports') }} as a

)

select
    *,
    -- Low + descending near the airport => inferred arrival; low + climbing => departure.
    -- Below 3000m (~10,000ft) and >1 m/s vertical rate keeps level cruise-through traffic
    -- (noisy near-zero vertical rate at altitude) out of both buckets.
    case
        when baro_altitude < 3000 and vertical_rate < -1 then 'arrival'
        when baro_altitude < 3000 and vertical_rate > 1 then 'departure'
        else null
    end as inferred_phase
from joined
where distance_km <= {{ var('airport_proximity_km') }}
