
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select skill
from PULSE.STAGING.stg_job_skills
where skill is null



  
  
      
    ) dbt_internal_test