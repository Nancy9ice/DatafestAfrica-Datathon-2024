
    
    

with child as (
    select student_course_id as from_field
    from DATAFESTAFRICA.CORE.fact_school_grades
    where student_course_id is not null
),

parent as (
    select student_course_id as to_field
    from DATAFESTAFRICA.INTERMEDIATE.stg_students_courses
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


