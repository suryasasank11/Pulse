-- One row per source system. md5 of the source name is the surrogate key.
select
    md5(source_name) as source_key,
    source_name,
    'api' as source_type
from (
    select distinct source as source_name
    from PULSE.STAGING.stg_jobs
    where source is not null
)