{{ config(materialized='view') }}

select
    s.source, d.day_of_week, sum(hit_counts) as total_hit_count
from
    {{ref('fact_hits')}} f 
    inner join {{ref('dim_sources')}} s on s.source_key = f.source_key
    inner join {{ref('dim_date')}} d on d.date_key = f.hit_date_key
group by all
order by s.source, d.day_of_week