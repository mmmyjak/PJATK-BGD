{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['flight_year', 'origin', 'dest']
) }}

with source_data as (
    select *
    from {{ source('silver', 'flights') }}
),
changed_keys as (
    {% if is_incremental() %}
    select distinct
        extract(year from "FL_DATE")::int as flight_year,
        "ORIGIN" as origin,
        "DEST" as dest
    from source_data
    where "_transformed_at" > (
        select coalesce(max(_aggregated_at), timestamp '1900-01-01')
        from {{ this }}
    )
    {% else %}
    select distinct
        extract(year from "FL_DATE")::int as flight_year,
        "ORIGIN" as origin,
        "DEST" as dest
    from source_data
    {% endif %}
),
rows_to_aggregate as (
    select sd.*
    from source_data sd
    inner join changed_keys ck
        on extract(year from sd."FL_DATE")::int = ck.flight_year
       and sd."ORIGIN" = ck.origin
       and sd."DEST" = ck.dest
)
select
    extract(year from "FL_DATE")::int as flight_year,
    "ORIGIN" as origin,
    "DEST" as dest,
    count(*) as total_flights_on_route,
    avg("AIR_TIME"::double precision) as avg_air_time,
    max("ARR_DELAY") as max_arrival_delay,
    current_timestamp as _aggregated_at
from rows_to_aggregate
group by
    extract(year from "FL_DATE")::int,
    "ORIGIN",
    "DEST"
