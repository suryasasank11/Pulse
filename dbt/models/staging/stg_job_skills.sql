select distinct posting_id, lower(trim(skill)) as skill from {{ source('pulse_raw', 'raw_job_skills') }} where posting_id is not null and skill is not null and length(trim(skill)) > 0
