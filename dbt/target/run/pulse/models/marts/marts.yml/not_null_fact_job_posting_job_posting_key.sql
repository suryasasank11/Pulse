
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select job_posting_key
from PULSE.MARTS.fact_job_posting
where job_posting_key is null



  
  
      
    ) dbt_internal_test