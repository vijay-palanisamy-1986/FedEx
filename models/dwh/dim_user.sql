{{ config(
        materialized='incremental'
        , unique_key = 'user_key'
        ) }}

{% set cols_user_key = ['user_id'] %}


with source_data as (
  select user_id, max(stg_inserted_dt) as stg_inserted_dt
  from (
      select user_id, stg_inserted_dt from {{ref('stg_fedex_case_data_web')}}
      union
      select user_id, stg_inserted_dt from {{ref('stg_fedex_case_data_app')}}
  )
  group by user_id
)

select distinct
  {{ dbt_utils.generate_surrogate_key(cols_user_key) }} as user_key
  , user_id
  , {{ dbt.current_timestamp() }}::timestamp with time zone as dwh_inserted_dt
from
  source_data
-- To avoid re-processing same staging data i.e., incase the stage data didnt change 
{% if is_incremental() %}
  where stg_inserted_dt > (select max(dwh_inserted_dt) from {{ this }})
{% endif %}