
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select role_key
from PULSE.MARTS.dim_role
where role_key is null



  
  
      
    ) dbt_internal_test