-- Typed cleanup of the raw OurAirports seed.
select
    icao_code,
    iata_code,
    name,
    type,
    cast(latitude_deg as double) as latitude,
    cast(longitude_deg as double) as longitude,
    cast(elevation_ft as double) as elevation_ft,
    iso_region,
    municipality
from {{ ref('airports') }}
where icao_code is not null
