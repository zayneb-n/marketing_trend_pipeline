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