
    
    

select
    url as unique_field,
    count(*) as n_records

from "marketing_pipeline"."public"."stg_articles"
where url is not null
group by url
having count(*) > 1


