{{ config(materialized='view') }}

with cte_hit_count_source as (
    select
        sum (hit_counts) as FedExWeb_source_total_hit_count        
    from
        {{ref('fact_hits')}} f 
        inner join {{ref('dim_sources')}} s on s.source_key = f.source_key and s.source = 'FedEx Web'
    group by ALL
)

, cte_hit_count_device as (
    select
        s.source
        , d.platform
        , d.device
        , sum (hit_counts) as device_total_hit_count
    from
        {{ref('fact_hits')}} f 
        inner join {{ref('dim_sources')}} s on s.source_key = f.source_key and s.source = 'FedEx Web'
        inner join {{ref('dim_devices')}} d on d.device_key = f.device_key
    group by ALL
)

select
    cte_hit_count_device.*
    , cte_hit_count_source.FedExWeb_source_total_hit_count
    , 100 * device_total_hit_count / FedExWeb_source_total_hit_count as percentage
from
    cte_hit_count_device
    inner join cte_hit_count_source
order by device_total_hit_count desc