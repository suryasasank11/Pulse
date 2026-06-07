
  create or replace   view PULSE.STAGING.stg_job_skills
  
  
  
  
  as (
    select distinct posting_id, lower(trim(skill)) as skill from PULSE.RAW.raw_job_skills where posting_id is not null and skill is not null and length(trim(skill)) > 0
  );

