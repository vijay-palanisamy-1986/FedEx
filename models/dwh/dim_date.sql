{{ config(
        materialized='incremental'
        , unique_key = 'date_key'
        ) }}

{% set cols_country_key = ['hit_date'] %}


with source_data as (
  select hit_date, max(stg_inserted_dt) as stg_inserted_dt
  from (  
      select hit_date, stg_inserted_dt from {{ref('stg_fedex_case_data_web')}}
      union
      select hit_date, stg_inserted_dt from {{ref('stg_fedex_case_data_app')}}
    )
  group by hit_date
)

select
  TO_VARCHAR(hit_date::date, 'YYYYMMDD') as date_key
  , hit_date as date
  , TO_VARCHAR(hit_date::date, 'YYYY') as year
  , TO_VARCHAR(hit_date::date, 'MM') as month_no
  , TO_VARCHAR(hit_date::date, 'MMMM') as month_full_name
  , TO_VARCHAR(hit_date::date, 'MON') as month_short_name
  , TO_VARCHAR(hit_date::date, 'DD') as day
  , TO_VARCHAR(hit_date::date, 'DY') as day_of_week
  , {{ dbt.current_timestamp() }}::timestamp with time zone as dwh_inserted_dt
from
  source_data
-- To avoid re-processing same staging data i.e., incase the stage data didnt change 
{% if is_incremental() %}
  where stg_inserted_dt > (select max(dwh_inserted_dt) from {{ this }})
{% endif %}