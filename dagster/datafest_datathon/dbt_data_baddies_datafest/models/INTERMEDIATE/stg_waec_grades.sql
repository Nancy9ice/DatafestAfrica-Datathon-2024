SELECT DISTINCT
    student_course_id,
    exam_year,
    waec_grade
FROM {{ source('data_baddies_datafest', 'raw_student_waec_grades') }}
