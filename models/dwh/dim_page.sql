{{ config(
        materialized='incremental'
        , unique_key = 'page_key'
        ) }}

{% set cols_page_key = ['page_name'] %}


with source_data as (
  select
    page_name
    , {{get_web_field_page_type('page_name')}} as page_type
    , max(stg_inserted_dt) as stg_inserted_dt
  from
    {{ref('stg_fedex_case_data_web')}}
  group by all
)

select distinct
  {{ dbt_utils.generate_surrogate_key(cols_page_key) }} as page_key
  , page_name
  , page_type
  , {{ dbt.current_timestamp() }}::timestamp with time zone as dwh_inserted_dt
from source_data
-- To avoid re-processing same staging data i.e., incase the stage data didnt change 
{% if is_incremental() %}
  where stg_inserted_dt > (select max(dwh_inserted_dt) from {{ this }})
{% endif %}