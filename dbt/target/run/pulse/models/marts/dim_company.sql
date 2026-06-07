
  
    

create or replace transient table PULSE.MARTS.dim_company
    
    
    
    as (-- One row per hiring company (missing names roll up to 'Unknown').
-- industry / size / startup are placeholders RemoteOK doesn't provide;
-- they get enriched when richer sources are added later.
select
    md5(company_name) as company_key,
    company_name,
    cast(null as varchar) as industry,
    cast(null as varchar) as company_size,
    cast(null as boolean) as is_startup
from (
    select distinct coalesce(company, 'Unknown') as company_name
    from PULSE.STAGING.stg_jobs
)
    )
;


  