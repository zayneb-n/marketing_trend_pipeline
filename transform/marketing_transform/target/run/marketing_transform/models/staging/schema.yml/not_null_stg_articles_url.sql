select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select url
from "marketing_pipeline"."public"."stg_articles"
where url is null



      
    ) dbt_internal_test