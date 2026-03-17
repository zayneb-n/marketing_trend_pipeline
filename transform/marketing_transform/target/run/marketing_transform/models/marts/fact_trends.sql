
  
    

  create  table "marketing_pipeline"."public"."fact_trends__dbt_tmp"
  
  
    as
  
  (
    select
    keyword,
    date,
    interest,
    extracted_at
from "marketing_pipeline"."public"."stg_trends"
  );
  