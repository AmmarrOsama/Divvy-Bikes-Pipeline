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
    FORMAT_TIMESTAMP('%A', started_at) AS DayOfWeek,
    ROUND(AVG(TIMESTAMP_DIFF(ended_at, started_at, MINUTE)),2) AS TripDurationMins
    from renamed
GROUP BY CustomerType, RideableType, DayOfWeek
ORDER BY CustomerType, RideableType, DayOfWeek
