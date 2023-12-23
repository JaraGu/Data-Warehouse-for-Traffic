{{ config(materialized='view') }}

with fastest_and_highest_mileage as (
    select *
    from {{ ref('fastest_vehicle_model') }}
    order by traveled_distance DESC
)

select *
from fastest_and_highest_mileage
