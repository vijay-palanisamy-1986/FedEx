{{ config( materialized='table' ) }}

select
  hit_date
  , geo_country
  , user_id
  , user_agent
  , 'FedEx App' as source
  , {{ dbt.current_timestamp() }}::timestamp with time zone as stg_inserted_dt
from
  {{ref('raw_fedex_case_data_app')}}