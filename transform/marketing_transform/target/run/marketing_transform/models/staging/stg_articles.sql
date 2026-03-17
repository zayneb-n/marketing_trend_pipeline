
  create view "marketing_pipeline"."public"."stg_articles__dbt_tmp"
    
    
  as (
    select
    keyword,
    title,
    source_name,
    published_at::date as published_date,
    url,
    description,
    extracted_at
from raw_articles
where title is not null
  );