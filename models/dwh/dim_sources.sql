{{ config(
        materialized='incremental'
        , unique_key = 'source_key'
        ) }}

{% set cols_source_key = ['source'] %}

with source_data as (
  select 'FedEx Web' as source, 'Web' as Source_Type
  union
  select 'FedEx App' as source, 'App' as Source_Type
)

select distinct
  {{ dbt_utils.generate_surrogate_key(cols_source_key) }} as source_key
  , source
  , source_type
  , {{ dbt.current_timestamp() }}::timestamp with time zone as dwh_inserted_dt
from
  source_data
