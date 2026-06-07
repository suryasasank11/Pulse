
    
    

with child as (
    select role_key as from_field
    from PULSE.MARTS.fact_job_posting
    where role_key is not null
),

parent as (
    select role_key as to_field
    from PULSE.MARTS.dim_role
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


