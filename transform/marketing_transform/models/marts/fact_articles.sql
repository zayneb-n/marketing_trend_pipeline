select
    keyword,
    title,
    source_name,
    published_date,
    url,
    description,
    extracted_at
from {{ ref('stg_articles') }}