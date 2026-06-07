-- One row per (posting, skill): the cleaned bridge grain.
with source as (
    select * from {{ source('pulse_raw', 'raw_job_skills') }}
),

cleaned as (
    select
        posting_id,
        lower(trim(skill)) as skill
    from source
    where posting_id is not null
      and skill is not null
      and length(trim(skill)) > 0
)

select distinct posting_id, skill
from cleaned