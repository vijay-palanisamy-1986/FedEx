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
    source
    , hit_date
    , geo_country
    , {{get_web_app_field_device('user_agent')}} as device
    , user_id
    , user_agent
    , page_name
    , {{get_web_field_browser_name('user_agent')}} as browser_name
    , hit_id
  from
    {{ref('stg_fedex_case_data_web')}}
  union
  select
    source
    , hit_date
    , geo_country
    , {{get_web_app_field_device('user_agent')}} as device
    , user_id
    , user_agent
    , null as page_name
    , null as browser_name
    , 1 as hit_id -- as there is no HIT id for APP and we assume 1 record per 1 hit is given in CSV file
  from
    {{ref('stg_fedex_case_data_app')}}
)

, source_data_final as (
  select
    {{ dbt_utils.generate_surrogate_key(cols_source_key) }} as source_key
    , TO_VARCHAR(hit_date::date, 'YYYYMMDD') as hit_date_key
    , {{ dbt_utils.generate_surrogate_key(cols_country_key) }} as country_key
    , {{ dbt_utils.generate_surrogate_key(cols_device_key) }} as device_key
    , {{ dbt_utils.generate_surrogate_key(cols_user_key) }} as user_key
    , {{ dbt_utils.generate_surrogate_key(cols_user_key_agent) }} as user_agent_key
    , case
          when source = 'FedEx Web' then {{ dbt_utils.generate_surrogate_key(cols_page_key) }}
          else NULL
      end as page_key
    , case
          when source = 'FedEx Web' then {{ dbt_utils.generate_surrogate_key(cols_browser_key) }}
          else NULL
      end as browser_key
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