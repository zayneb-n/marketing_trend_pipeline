select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select keyword
from "marketing_pipeline"."public"."stg_articles"
where keyword is null



      
    ) dbt_internal_test