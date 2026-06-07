-- Calendar dimension: one row per day, 2020-01-01 through 2030-12-31.
-- date_key is a smart integer (YYYYMMDD) that the facts join on directly.
with dates as (
    select dateadd(day, seq4(), to_date('2020-01-01')) as full_date
    from table(generator(rowcount => 4018))
)
select
    cast(to_char(full_date, 'YYYYMMDD') as integer) as date_key,
    full_date,
    year(full_date)                                 as year,
    quarter(full_date)                              as quarter,
    month(full_date)                                as month,
    monthname(full_date)                            as month_name,
    week(full_date)                                 as week_of_year,
    dayofweek(full_date)                            as day_of_week,
    dayname(full_date)                              as day_name,
    case when dayofweek(full_date) in (0, 6) then true else false end as is_weekend
from dates
