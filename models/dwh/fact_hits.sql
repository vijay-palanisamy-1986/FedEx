{{ config(
        materialized='incremental'
        , unique_key = 'hit_key'
        ) }}

{% set cols_source_key = ['source'] %}
{% set cols_country_key = ['geo_country'] %}
{% set cols_device_key = ['device'] %}
{% set cols_user_key = ['user_id'] %}
{% set cols_user_key_agent = ['user_agent'] %}
{% set cols_page_key = ['page_name'] %}
{% set cols_browser_key = ['browser_name'] %}
{% set cols_hit_key = ['source_key', 'hit_date_key', 'country_key','device_key','user_key','user_agent_key','page_key', 'browser_key'] %}


with source_data as (
  select
    {{ dbt_utils.generate_surrogate_key(cols_source_key) }} as source_key
    , TO_VARCHAR(hit_date::date, 'YYYYMMDD') as hit_date_key
    , {{ dbt_utils.generate_surrogate_key(cols_country_key) }} as country_key
    , {{ dbt_utils.generate_surrogate_key(cols_device_key) }} as device_key
    , {{ dbt_utils.generate_surrogate_key(cols_user_key) }} as user_key
    , {{ dbt_utils.generate_surrogate_key(cols_user_key_agent) }} as user_agent_key
    , {{ dbt_utils.generate_surrogate_key(cols_page_key) }} as page_key
    , {{ dbt_utils.generate_surrogate_key(cols_browser_key) }} as browser_key
    , hit_id
  from
    {{ref('stg_fedex_case_data_web')}}
  union
  select
    {{ dbt_utils.generate_surrogate_key(cols_source_key) }} as source_key
    , TO_VARCHAR(hit_date::date, 'YYYYMMDD') as date_key
    , {{ dbt_utils.generate_surrogate_key(cols_country_key) }} as country_key
    , {{ dbt_utils.generate_surrogate_key(cols_device_key) }} as device_key
    , {{ dbt_utils.generate_surrogate_key(cols_user_key) }} as user_key
    , {{ dbt_utils.generate_surrogate_key(cols_user_key_agent) }} as user_agent_key
    , null as browser_key
    , null as page_key    
    , 1 as hit_id -- as there is no HIT id for APP and we assume 1 record per 1 hit is given in CSV file
  from
    {{ref('stg_fedex_case_data_app')}}
)

, source_data_final as (
  select
    source_key
    , hit_date_key
    , country_key
    , device_key
    , user_key
    , user_agent_key
    , page_key
    , browser_key
    , count(hit_id) hit_counts
  from
    source_data
  group by all
)

select
  {{ dbt_utils.generate_surrogate_key(cols_hit_key) }} as hit_key
  , * 
  , {{ dbt.current_timestamp() }}::timestamp with time zone as dwh_inserted_dt
 from source_data_final