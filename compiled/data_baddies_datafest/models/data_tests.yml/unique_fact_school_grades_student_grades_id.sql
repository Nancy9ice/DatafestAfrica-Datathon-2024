
    
    

select
    student_grades_id as unique_field,
    count(*) as n_records

from DATAFESTAFRICA.CORE.fact_school_grades
where student_grades_id is not null
group by student_grades_id
having count(*) > 1


