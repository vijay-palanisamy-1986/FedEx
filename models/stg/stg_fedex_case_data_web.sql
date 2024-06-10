{{ config( materialized='table' ) }}


with source_data as (
  select
    *
    , substr(user_Agent, 14, position( ';' IN user_agent) - 14) as device_raw
  from {{ref('raw_fedex_case_data_web')}}
)

, source_data_final as (
  select
    hit_id
    , hit_date
    , geo_country
    , user_id
    , user_agent
    , pagename as page_name
    , case
        when lower(pagename) like '%home%' then 'Home'
        when lower(pagename) like '%login%' then 'Login/Logout'
        when lower(pagename) like '%sign%' then 'Login/Logout'
        when lower(pagename) like '%cust%' then 'Customer/User-Account'
        when lower(pagename) like '%account%' then 'Customer/User-Account'
        when lower(pagename) like '%user%' then 'Customer/User-Account'
        when lower(pagename) like '%card%' then 'Customer/User-Account'
        when lower(pagename) like '%track%' then 'Tracking'
        when lower(pagename) like '%search%' then 'Tracking'
        when lower(pagename) like '%trkid%' then 'Tracking'
        when lower(pagename) like '%notfound%' then 'Tracking'
        when lower(pagename) like '%sms%' then 'Tracking'
        when lower(pagename) like '%ship%' then 'Shipment/Delivery'
        when lower(pagename) like '%addres%' then 'Shipment/Delivery'
        when lower(pagename) like '%deliv%' then 'Shipment/Delivery'
        when lower(pagename) like '%bill%' then 'Billing'
        when lower(pagename) like '%invoice%' then 'Billing'
        when lower(pagename) like '%print%' then 'Print'
        when lower(pagename) like '%checkout%' then 'Checkout'
        when lower(pagename) like '%cart%' then 'Checkout'
        when lower(pagename) like '%rate%' then 'Rate'
        when lower(pagename) like '%contact%' then 'Contact-Us'
        when lower(pagename) like '%error%' then 'Error'
        when lower(pagename) like '%fedex/apps%' then 'FedEx App in Web'
        else 'Others'
      end as page_type
    , case
        when lower(device_raw) like '%android%' then 'Android'
        when lower(device_raw) like '%iphone%' then 'iPhone's
        when lower(device_raw) like '%ipad%' then 'iPad'
        when lower(device_raw) like '%ipod%' then 'iPod'
        when lower(device_raw) like '%macintosh%' then 'Macintosh'
        when lower(device_raw) like '%linux%' then 'Linux'
        when lower(device_raw) like '%windows%' then 'Windows'
        else 'Others'
      end as device
    , 'FedEx Web' as source
    , {{ dbt.current_timestamp() }}::timestamp with time zone as stg_inserted_dt
  from
    source_data
)

select * from source_data_final