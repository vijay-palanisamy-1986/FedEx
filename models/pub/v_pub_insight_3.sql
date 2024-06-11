{{ config(materialized='view') }}

with cte_hit_count_source as (
    select
        sum (hit_counts) as FedExWeb_source_total_hit_count        
    from
        {{ref('fact_hits')}} f 
        inner join {{ref('dim_sources')}} s on s.source_key = f.source_key and s.source = 'FedEx Web'
    group by ALL
)

, cte_hit_count_page as (
    select
        s.source, p.page_type, sum(hit_counts) as page_total_hit_count
    from
        {{ref('fact_hits')}} f
        inner join {{ref('dim_sources')}} s on s.source_key = f.source_key and s.source = 'FedEx Web'
        inner join {{ref('dim_page')}} p on p.page_key = f.page_key
    group by all
)

select
    cte_hit_count_page.*
    , cte_hit_count_source.FedExWeb_source_total_hit_count
    , 100 * page_total_hit_count / FedExWeb_source_total_hit_count as percentage
from
    cte_hit_count_page
    inner join cte_hit_count_source
order by page_total_hit_count desc