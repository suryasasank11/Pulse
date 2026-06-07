-- Grain: ONE ROW PER JOB POSTING.
-- Each *_key is md5() of the exact natural value the matching dimension hashed,
-- so the fact lines up with its dimensions without database-enforced FKs.
-- date_key is the smart YYYYMMDD integer that joins to dim_date.
select
    md5(posting_id)                                 as job_posting_key,
    posting_id,

    -- foreign keys into the dimensions
    md5(coalesce(company, 'Unknown'))               as company_key,
    md5(coalesce(location_raw, 'Unknown'))          as location_key,
    md5(source)                                     as source_key,
    md5(title)                                      as role_key,
    cast(to_char(posted_at, 'YYYYMMDD') as integer) as date_key,

    -- measures / degenerate attributes
    salary_min_usd,
    salary_max_usd,
    salary_avg_usd,
    is_remote,
    job_url,
    posted_at
from PULSE.STAGING.stg_jobs