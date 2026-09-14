-- Bronze -> silver: typed, de-duplicated, junk-filtered state vectors.
with source as (

    select *
    from read_parquet(
        '{{ env_var("FLIGHTSTREAM_DATA_DIR", "../data") }}/bronze/**/*.parquet',
        hive_partitioning = true,
        union_by_name = true
    )

),

cleaned as (

    select
        icao24,
        trim(callsign) as callsign,
        origin_country,
        time_position,
        last_contact,
        cast(latitude as double) as latitude,
        cast(longitude as double) as longitude,
        cast(baro_altitude as double) as baro_altitude,
        cast(geo_altitude as double) as geo_altitude,
        on_ground,
        cast(velocity as double) as velocity_ms,
        cast(velocity as double) * 1.94384 as velocity_knots,
        cast(true_track as double) as heading,
        cast(vertical_rate as double) as vertical_rate,
        squawk,
        position_source,
        fetched_at,
        fetched_at as snapshot_ts,
        row_number() over (
            partition by icao24, fetched_at
            order by last_contact desc
        ) as _dedupe_rank
    from source
    where latitude is not null
      and longitude is not null
      and on_ground = false

)

select
    icao24,
    callsign,
    origin_country,
    time_position,
    last_contact,
    latitude,
    longitude,
    baro_altitude,
    geo_altitude,
    on_ground,
    velocity_ms,
    velocity_knots,
    heading,
    vertical_rate,
    squawk,
    position_source,
    fetched_at,
    snapshot_ts
from cleaned
where _dedupe_rank = 1
