select
    keyword,
    date,
    interest,
    extracted_at
from "marketing_pipeline"."public"."raw_trends"
where interest is not null