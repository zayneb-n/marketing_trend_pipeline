
  
    

  create  table "marketing_pipeline"."public"."dim_date__dbt_tmp"
  
  
    as
  
  (
    select distinct
    date,
    extract(year from date)    as year,
    extract(month from date)   as month,
    extract(week from date)    as week_number,
    to_char(date, 'Month')     as month_name,
    to_char(date, 'Day')       as day_name
from "marketing_pipeline"."public"."stg_trends"
  );
  