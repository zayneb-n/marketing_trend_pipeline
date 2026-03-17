
  create view "marketing_pipeline"."public"."stg_trends__dbt_tmp"
    
    
  as (
    select
    keyword,
    date,
    interest,
    extracted_at
from "marketing_pipeline"."public"."raw_trends"
where interest is not null
  );