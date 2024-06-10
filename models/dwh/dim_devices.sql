{{ config(
        materialized='incremental'
        , unique_key = 'device_key'
        ) }}

{% set cols_device_key = ['device'] %}

with source_data as (
  select 'Android' as device, 'Android' as platform
  union select 'iPhone' as device, 'Apple' as platform
  union select 'iPad' as device, 'Apple' as platform
  union select 'iPod' as device, 'Apple' as platform
  union select 'Macintosh' as device, 'Apple' as platform
  union select 'Linux' as device, 'Linux' as platform
  union select 'Windows' as device, 'Windows' as platform
  union select 'Others' as device, 'Others' as platform
)

select
  {{ dbt_utils.generate_surrogate_key(cols_device_key) }} as device_key
  , device
  , platform
  , {{ dbt.current_timestamp() }}::timestamp with time zone as dwh_inserted_dt
from
  source_data