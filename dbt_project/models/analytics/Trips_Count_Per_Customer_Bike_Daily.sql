with 

source as (

    select * from {{ source('analytics', 'bikes_managed') }}

),

renamed as (

    select
        ride_id,
        rideable_type,
        started_at,
        ended_at,
        start_station_name,
        start_station_id,
        end_station_name,
        end_station_id,
        start_lat,
        start_lng,
        end_lat,
        end_lng,
        member_casual

    from source

)

select 
    member_casual AS CustomerType,
    rideable_type AS RideableType,
    DATE_TRUNC(started_at, DAY) AS Day,
    COUNT(*) AS TripsCount
    from renamed
GROUP BY Day, CustomerType, RideableType
ORDER BY Day, CustomerType, RideableType
