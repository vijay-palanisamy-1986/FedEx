{{ config(
        materialized='incremental'
        , unique_key = 'browser_key'
        ) }}

{% set cols_browser_key = ['browser_name'] %}


with source_data as (
  select
    {{get_web_field_browser_name('user_agent')}} as browser_name
    , max(stg_inserted_dt) as stg_inserted_dt
  from {{ref('stg_fedex_case_data_web')}}
  group by all
  order by 1
)

select
  {{ dbt_utils.generate_surrogate_key(cols_browser_key) }} as browser_key
  , browser_name
  , {{ dbt.current_timestamp() }}::timestamp with time zone as dwh_inserted_dt
from source_data
-- To avoid re-processing same staging data i.e., incase the stage data didnt change 
{% if is_incremental() %}
  where stg_inserted_dt > (select max(dwh_inserted_dt) from {{ this }})
{% endif %}