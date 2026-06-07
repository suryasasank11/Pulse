
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select posting_id
from PULSE.STAGING.stg_jobs
where posting_id is null



  
  
      
    ) dbt_internal_test