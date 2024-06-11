
-- To test if there are any data for new contries that are not configured in our dim_country table
select *
from {{ ref('stg_fedex_case_data_app')}}
where geo_country not in ( select country_name_short from {{ref('dim_country')}} )
--where lower(geo_country) not in ( select lower(country_name_short) from {{ref('dim_country')}} )