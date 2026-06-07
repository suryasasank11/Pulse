-- One row per distinct skill, with a light keyword-based category.
select
    md5(skill_name) as skill_key,
    skill_name,
    case
        when skill_name in ('aws','gcp','azure','cloud','kubernetes','docker','terraform') then 'cloud'
        when skill_name in ('python','sql','java','javascript','typescript','go','scala','rust','ruby','c++') then 'language'
        when skill_name in ('pytorch','tensorflow','spark','airflow','kafka','react','django','flask','pandas') then 'framework'
        when skill_name in ('postgres','postgresql','mysql','mongodb','snowflake','redis') then 'database'
        else 'other'
    end as skill_category
from (
    select distinct skill as skill_name
    from {{ ref('stg_job_skills') }}
    where skill is not null
)