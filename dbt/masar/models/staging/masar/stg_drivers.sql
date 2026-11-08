select
    cast(driver_id as string)       as driver_id,
    lower(trim(vehicle_type))       as vehicle_type,
    cast(driver_rating as double)   as driver_rating,
    initcap(trim(city))             as city
from {{ source('bronze', 'drivers') }}
where driver_id is not null