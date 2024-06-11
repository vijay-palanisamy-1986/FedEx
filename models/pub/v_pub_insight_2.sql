{{ config(materialized='view') }}

with cte_hit_count_source as (
    select
        sum (hit_counts) as FedExWeb_source_total_hit_count        
    from
        {{ref('fact_hits')}} f 
        inner join {{ref('dim_sources')}} s on s.source_key = f.source_key and s.source = 'FedEx Web'
    group by ALL
)

, cte_hit_count_browser as (
    select
        s.source, b.browser_name, sum(hit_counts) as browser_total_hit_count
    from
        {{ref('fact_hits')}} f
        inner join {{ref('dim_sources')}} s on s.source_key = f.source_key and s.source = 'FedEx Web'
        inner join {{ref('dim_browser')}} b on b.browser_key = f.browser_key
    group by all
)

select
    cte_hit_count_browser.*
    , cte_hit_count_source.FedExWeb_source_total_hit_count
    , 100 * browser_total_hit_count / FedExWeb_source_total_hit_count as percentage
from
    cte_hit_count_browser
    inner join cte_hit_count_source
order by browser_total_hit_count desc