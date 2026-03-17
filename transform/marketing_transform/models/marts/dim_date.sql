select distinct
    date,
    extract(year from date)    as year,
    extract(month from date)   as month,
    extract(week from date)    as week_number,
    to_char(date, 'Month')     as month_name,
    to_char(date, 'Day')       as day_name
from {{ ref('stg_trends') }}