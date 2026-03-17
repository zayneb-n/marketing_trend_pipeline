select
    keyword,
    date,
    interest,
    extracted_at
from {{ source('public', 'raw_trends') }}
where interest is not null