{{ config(
        materialized='incremental'
        , unique_key = 'user_agent_key'
        ) }}

{% set cols_user_key_agent = ['user_agent'] %}


with source_data as (
  select user_agent, max(stg_inserted_dt) as stg_inserted_dt
  from (
      select user_agent, stg_inserted_dt from {{ref('stg_fedex_case_data_web')}}
      union
      select user_agent, stg_inserted_dt from {{ref('stg_fedex_case_data_app')}}
  )
  group by user_agent
)

select
  {{ dbt_utils.generate_surrogate_key(cols_user_key_agent) }} as user_agent_key
  , user_agent    
  , {{ dbt.current_timestamp() }}::timestamp with time zone as dwh_inserted_dt
from
    source_data
-- To avoid re-processing same staging data i.e., incase the stage data didnt change 
{% if is_incremental() %}
  where stg_inserted_dt > (select max(dwh_inserted_dt) from {{ this }})
{% endif %}