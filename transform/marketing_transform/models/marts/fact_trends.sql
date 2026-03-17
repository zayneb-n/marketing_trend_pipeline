select
    keyword,
    date,
    interest,
    extracted_at
from {{ ref('stg_trends') }}