-- Grain: ONE ROW PER (POSTING, SKILL). The bridge that resolves the
-- many-to-many between postings and skills.
-- job_posting_key links to fact_job_posting; skill_key links to dim_skill.
select
    md5(posting_id) as job_posting_key,
    md5(skill)      as skill_key,
    posting_id,
    skill
from {{ ref('stg_job_skills') }}