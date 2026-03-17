
  
    

  create  table "marketing_pipeline"."public"."fact_articles__dbt_tmp"
  
  
    as
  
  (
    select
    keyword,
    title,
    source_name,
    published_date,
    url,
    description,
    extracted_at
from "marketing_pipeline"."public"."stg_articles"
  );
  