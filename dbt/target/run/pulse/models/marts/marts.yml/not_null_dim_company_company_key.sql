
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select company_key
from PULSE.MARTS.dim_company
where company_key is null



  
  
      
    ) dbt_internal_test