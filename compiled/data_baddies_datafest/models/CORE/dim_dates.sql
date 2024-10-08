WITH date_dim AS (
    
    
with base_dates as (
    
    with date_spine as
(

    





with rawdata as (

    

    

    with p as (
        select 0 as generated_number union all select 1
    ), unioned as (

    select

    
    p0.generated_number * power(2, 0)
     + 
    
    p1.generated_number * power(2, 1)
     + 
    
    p2.generated_number * power(2, 2)
     + 
    
    p3.generated_number * power(2, 3)
     + 
    
    p4.generated_number * power(2, 4)
     + 
    
    p5.generated_number * power(2, 5)
     + 
    
    p6.generated_number * power(2, 6)
     + 
    
    p7.generated_number * power(2, 7)
     + 
    
    p8.generated_number * power(2, 8)
     + 
    
    p9.generated_number * power(2, 9)
     + 
    
    p10.generated_number * power(2, 10)
     + 
    
    p11.generated_number * power(2, 11)
     + 
    
    p12.generated_number * power(2, 12)
    
    
    + 1
    as generated_number

    from

    
    p as p0
     cross join 
    
    p as p1
     cross join 
    
    p as p2
     cross join 
    
    p as p3
     cross join 
    
    p as p4
     cross join 
    
    p as p5
     cross join 
    
    p as p6
     cross join 
    
    p as p7
     cross join 
    
    p as p8
     cross join 
    
    p as p9
     cross join 
    
    p as p10
     cross join 
    
    p as p11
     cross join 
    
    p as p12
    
    

    )

    select *
    from unioned
    where generated_number <= 5509
    order by generated_number



),

all_periods as (

    select (
        

    dateadd(
        day,
        (row_number() over (order by 1) - 1),
        cast('2010-01-01' as timestamp)
        )


    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= cast('2025-01-31' as timestamp)

)

select * from filtered



)
select
    cast(d.date_day as timestamp) as date_day
from
    date_spine d


),
dates_with_prior_year_dates as (

    select
        cast(d.date_day as date) as date_day,
        cast(

    dateadd(
        year,
        -1,
        d.date_day
        )

 as date) as prior_year_date_day,
        cast(

    dateadd(
        day,
        -364,
        d.date_day
        )

 as date) as prior_year_over_year_date_day
    from
    	base_dates d

)
select
    d.date_day,
    cast(

    dateadd(
        day,
        -1,
        d.date_day
        )

 as date) as prior_date_day,
    cast(

    dateadd(
        day,
        1,
        d.date_day
        )

 as date) as next_date_day,
    d.prior_year_date_day as prior_year_date_day,
    d.prior_year_over_year_date_day,
    case
            when date_part('dayofweek', d.date_day) = 7 then 1
            else date_part('dayofweek', d.date_day) + 1
        end as day_of_week,
    date_part('dayofweekiso', d.date_day) as day_of_week_iso,
    -- long version not implemented on Snowflake so we're doing it manually :/
    case dayname(d.date_day)
        when 'Mon' then 'Monday'
        when 'Tue' then 'Tuesday'
        when 'Wed' then 'Wednesday'
        when 'Thu' then 'Thursday'
        when 'Fri' then 'Friday'
        when 'Sat' then 'Saturday'
        when 'Sun' then 'Sunday'
    end as day_of_week_name,
    dayname(d.date_day) as day_of_week_name_short,
    date_part('day', d.date_day) as day_of_month,
    date_part('dayofyear', d.date_day) as day_of_year,

    
    
    cast(

    dateadd(
        day,
        -1 * (case
            when date_part('dayofweek', d.date_day) = 7 then 1
            else date_part('dayofweek', d.date_day) + 1
        end - 1),
        d.date_day
        )

 as date) as week_start_date,
    cast(

    dateadd(
        day,
        6,
        
    
    cast(

    dateadd(
        day,
        -1 * (case
            when date_part('dayofweek', d.date_day) = 7 then 1
            else date_part('dayofweek', d.date_day) + 1
        end - 1),
        d.date_day
        )

 as date)
        )

 as date) as week_end_date,
    
    
    cast(

    dateadd(
        day,
        -1 * (case
            when date_part('dayofweek', d.prior_year_over_year_date_day) = 7 then 1
            else date_part('dayofweek', d.prior_year_over_year_date_day) + 1
        end - 1),
        d.prior_year_over_year_date_day
        )

 as date) as prior_year_week_start_date,
    cast(

    dateadd(
        day,
        6,
        
    
    cast(

    dateadd(
        day,
        -1 * (case
            when date_part('dayofweek', d.prior_year_over_year_date_day) = 7 then 1
            else date_part('dayofweek', d.prior_year_over_year_date_day) + 1
        end - 1),
        d.prior_year_over_year_date_day
        )

 as date)
        )

 as date) as prior_year_week_end_date,
    cast(date_part('week', d.date_day) as integer) as week_of_year,

    cast(date_trunc('week', d.date_day) as date) as iso_week_start_date,
    cast(

    dateadd(
        day,
        6,
        cast(date_trunc('week', d.date_day) as date)
        )

 as date) as iso_week_end_date,
    cast(date_trunc('week', d.prior_year_over_year_date_day) as date) as prior_year_iso_week_start_date,
    cast(

    dateadd(
        day,
        6,
        cast(date_trunc('week', d.prior_year_over_year_date_day) as date)
        )

 as date) as prior_year_iso_week_end_date,
    cast(date_part('weekiso', d.date_day) as integer) as iso_week_of_year,

    cast(date_part('week', d.prior_year_over_year_date_day) as integer) as prior_year_week_of_year,
    cast(date_part('weekiso', d.prior_year_over_year_date_day) as integer) as prior_year_iso_week_of_year,

    cast(date_part('month', d.date_day) as integer) as month_of_year,
    to_char(d.date_day, 'MMMM')  as month_name,
    to_char(d.date_day, 'MON')  as month_name_short,

    cast(date_trunc('month', d.date_day) as date) as month_start_date,
    cast(cast(
        

    dateadd(
        day,
        -1,
        

    dateadd(
        month,
        1,
        date_trunc('month', d.date_day)
        )


        )


        as date) as date) as month_end_date,

    cast(date_trunc('month', d.prior_year_date_day) as date) as prior_year_month_start_date,
    cast(cast(
        

    dateadd(
        day,
        -1,
        

    dateadd(
        month,
        1,
        date_trunc('month', d.prior_year_date_day)
        )


        )


        as date) as date) as prior_year_month_end_date,

    cast(date_part('quarter', d.date_day) as integer) as quarter_of_year,
    cast(date_trunc('quarter', d.date_day) as date) as quarter_start_date,
    cast(cast(
        

    dateadd(
        day,
        -1,
        

    dateadd(
        quarter,
        1,
        date_trunc('quarter', d.date_day)
        )


        )


        as date) as date) as quarter_end_date,

    cast(date_part('year', d.date_day) as integer) as year_number,
    cast(date_trunc('year', d.date_day) as date) as year_start_date,
    cast(cast(
        

    dateadd(
        day,
        -1,
        

    dateadd(
        year,
        1,
        date_trunc('year', d.date_day)
        )


        )


        as date) as date) as year_end_date
from
    dates_with_prior_year_dates d
order by 1


)

SELECT * FROM date_dim