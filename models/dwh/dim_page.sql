{{ config(
        materialized='incremental'
        , unique_key = 'page_key'
        ) }}

{% set cols_page_key = ['page_name'] %}


with source_data as (
  select page_name, page_type, max(stg_inserted_dt) as stg_inserted_dt
  from {{ref('stg_fedex_case_data_web')}}
  group by page_name, page_type
)

select distinct
  {{ dbt_utils.generate_surrogate_key(cols_page_key) }} as page_key
  , page_name
  , page_type
  , stg_inserted_dt
  , {{ dbt.current_timestamp() }}::timestamp with time zone as dwh_inserted_dt
from source_data
-- To avoid re-processing same staging data i.e., incase the stage data didnt change 
{% if is_incremental() %}
  where stg_inserted_dt > (select max(dwh_inserted_dt) from {{ this }})
{% endif %}