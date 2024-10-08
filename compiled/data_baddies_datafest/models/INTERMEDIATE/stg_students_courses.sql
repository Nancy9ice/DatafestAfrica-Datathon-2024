SELECT DISTINCT
    student_course_id,
    student_id,
    course_id,
    grade_level_id as class_id,
    created_at,
    TO_CHAR(CAST(created_at AS DATE), 'YYYYMMDD') AS created_date_id,
    updated_at,
    TO_CHAR(CAST(updated_at AS DATE), 'YYYYMMDD') AS updated_date_id
FROM datafestafrica.RAW.raw_students_courses