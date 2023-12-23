{{ config(materialized='view') }}

with fastest_data as (
    select * from {{ ref('fastest_vehicle_model') }}
    where avg_speed > 40
),

highest_mileage_data as (
    select * from vehicles
    where traveled_distance > 375
)

select *
from fastest_data
union all
select *
from highest_mileage_data;
