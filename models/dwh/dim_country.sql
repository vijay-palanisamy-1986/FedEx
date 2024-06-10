{{ config(
        materialized='incremental'
        , unique_key = 'country_key'
        ) }}

{% set cols_country_key = ['country_name_short'] %}


with source_data as (
  select 'United States of America' as country_name, 'USA' as country_name_short, 'NA' as region
  union
  select 'Netherlands' as country_name, 'NL' as country_name_short, 'EU' as region
)

select
  {{ dbt_utils.generate_surrogate_key(cols_country_key) }} as country_key
  , country_name
  , country_name_short
  , region
  , {{ dbt.current_timestamp() }}::timestamp with time zone as dwh_inserted_dt
from
  source_data