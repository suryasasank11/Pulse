-- One row per distinct job title, with role attributes inferred from the
-- free-text title via keyword rules (RemoteOK gives no structured role data).
-- role_key = md5(title); the fact re-hashes the same title to link back here.
select
    md5(title) as role_key,
    title as role_title,
    case
        when lower(title) like '%machine learning%' or lower(title) like '%ml engineer%' then 'ML Engineer'
        when lower(title) like '%data engineer%'    then 'Data Engineer'
        when lower(title) like '%data scientist%' or lower(title) like '%data science%' then 'Data Scientist'
        when lower(title) like '%data analyst%' or lower(title) like '%analytics%'       then 'Data Analyst'
        when lower(title) like '%ai engineer%' or lower(title) like '%artificial intelligence%' then 'AI Engineer'
        when lower(title) like '%software engineer%' or lower(title) like '%developer%' then 'Software Engineer'
        when lower(title) like '%devops%' or lower(title) like '%platform engineer%' or lower(title) like '%infrastructure%' then 'DevOps / Platform'
        when lower(title) like '%product manager%' or lower(title) like '%product owner%' then 'Product'
        else 'Other'
    end as role_family,
    case
        when lower(title) like '%intern%'    then 'Intern'
        when lower(title) like '%principal%' or lower(title) like '%staff%' or lower(title) like '%lead%' then 'Lead/Principal'
        when lower(title) like '%senior%' or lower(title) like '%sr.%' or lower(title) like '%sr %' then 'Senior'
        when lower(title) like '%junior%' or lower(title) like '%jr.%' or lower(title) like '%entry%' or lower(title) like '%associate%' then 'Junior'
        else 'Mid'
    end as seniority_level,
    case
        when lower(title) like '%contract%' or lower(title) like '%contractor%' then 'Contract'
        when lower(title) like '%part-time%' or lower(title) like '%part time%' then 'Part-time'
        when lower(title) like '%intern%' then 'Internship'
        else 'Full-time'
    end as employment_type
from (
    select distinct title
    from PULSE.STAGING.stg_jobs
    where title is not null
)