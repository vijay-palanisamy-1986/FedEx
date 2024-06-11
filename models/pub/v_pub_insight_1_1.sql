{{ config(materialized='view') }}

select
    source    
    , source_total_hit_count
    , grand_total_hit_count
    , 100 * source_total_hit_count / grand_total_hit_count as percentage
from
    (
        select
            s.source
            , sum(hit_counts) as source_total_hit_count
        from
            {{ref('fact_hits')}} f    
            inner join {{ref('dim_sources')}} s on s.source_key = f.source_key
        group by s.source
    ) cte
    inner join (select sum(hit_counts) grand_total_hit_count from {{ref('fact_hits')}} ) cte_thc on 1=1