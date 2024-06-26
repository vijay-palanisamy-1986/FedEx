{{ config(materialized='view') }}

with cte_hit_count_source as (
    select
        sum (hit_counts) as FedExApp_source_total_hit_count        
    from
        {{ref('fact_hits')}} f 
        inner join {{ref('dim_sources')}} s on s.source_key = f.source_key and s.source = 'FedEx App'
    group by ALL
)

, cte_hit_count_platform as (
    select
        s.source, d.platform, sum(hit_counts) as platform_total_hit_count
    from
        {{ref('fact_hits')}} f
        inner join {{ref('dim_sources')}} s on s.source_key = f.source_key and s.source = 'FedEx App'
        inner join {{ref('dim_devices')}} d on d.device_key = f.device_key
    group by all
)

select
    cte_hit_count_platform.*
    , cte_hit_count_source.FedExApp_source_total_hit_count
    , 100 * platform_total_hit_count / FedExApp_source_total_hit_count as percentage
from
    cte_hit_count_platform
    inner join cte_hit_count_source
order by platform_total_hit_count desc