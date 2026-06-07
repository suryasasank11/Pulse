
    
    

select
    job_posting_key as unique_field,
    count(*) as n_records

from PULSE.MARTS.fact_job_posting
where job_posting_key is not null
group by job_posting_key
having count(*) > 1


