{{ config( materialized='table' ) }}


with source_data as (
  select
    *
    , substr(user_Agent, 14, position( ';' IN user_agent) - 14) as device_raw
  from
    {{ref('raw_fedex_case_data_app')}}

)
, source_data_final as (
  select
    hit_date
    , geo_country
    , user_id
    , user_agent
    , case
        when lower(device_raw) like '%android%' then 'Android'
        when lower(device_raw) like '%iphone%' then 'iPhone'
        when lower(device_raw) like '%ipad%' then 'iPad'
        when lower(device_raw) like '%ipod%' then 'iPod'
        when lower(device_raw) like '%macintosh%' then 'Macintosh'
        when lower(device_raw) like '%linux%' then 'Linux'
        when lower(device_raw) like '%windows%' then 'Windows'
        else 'Others'
      end as device
    , 'FedEx App' as source
    , {{ dbt.current_timestamp() }}::timestamp with time zone as stg_inserted_dt
  from
    source_data
)

select * from source_data_final