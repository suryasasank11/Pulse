-- One row per distinct location string (missing -> 'Unknown').
-- RemoteOK roles are all remote, so is_remote is true; city/state parsing
-- comes later with sources that provide structured locations.
select
    md5(location_name) as location_key,
    location_name,
    true as is_remote
from (
    select distinct coalesce(location_raw, 'Unknown') as location_name
    from {{ ref('stg_jobs') }}
)