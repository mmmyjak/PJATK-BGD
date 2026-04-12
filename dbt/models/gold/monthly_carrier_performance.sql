{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['flight_year', 'flight_month', 'op_carrier']
) }}

with source_data as (
    select *
    from {{ source('silver', 'flights') }}
),
changed_keys as (
    {% if is_incremental() %}
    select distinct
        extract(year from "FL_DATE")::int as flight_year,
        extract(month from "FL_DATE")::int as flight_month,
        "OP_CARRIER" as op_carrier
    from source_data
    where "_transformed_at" > (
        select coalesce(max(_aggregated_at), timestamp '1900-01-01')
        from {{ this }}
    )
    {% else %}
    select distinct
        extract(year from "FL_DATE")::int as flight_year,
        extract(month from "FL_DATE")::int as flight_month,
        "OP_CARRIER" as op_carrier
    from source_data
    {% endif %}
),
rows_to_aggregate as (
    select sd.*
    from source_data sd
    inner join changed_keys ck
        on extract(year from sd."FL_DATE")::int = ck.flight_year
       and extract(month from sd."FL_DATE")::int = ck.flight_month
       and sd."OP_CARRIER" = ck.op_carrier
)
select
    extract(year from "FL_DATE")::int as flight_year,
    extract(month from "FL_DATE")::int as flight_month,
    "OP_CARRIER" as op_carrier,
    count(*) as total_flights,
    sum("CANCELLED") as cancelled_flights,
    avg("DEP_DELAY") as avg_departure_delay,
    sum("WEATHER_DELAY") as total_weather_delay_mins,
    current_timestamp as _aggregated_at
from rows_to_aggregate
group by
    extract(year from "FL_DATE")::int,
    extract(month from "FL_DATE")::int,
    "OP_CARRIER"
