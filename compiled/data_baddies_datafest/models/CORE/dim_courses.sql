SELECT
    course_id,
    class_short_name,
    class_title,
    full_name as teacher,
    course_title,
    course_short_name,
    course_department,
    total_teaching_hours_period,
    course_description,
    course_created_at,
    course_created_date_id,
    course_updated_at,
    course_updated_date_id
FROM DATAFESTAFRICA.INTERMEDIATE.stg_courses courses
LEFT JOIN DATAFESTAFRICA.INTERMEDIATE.stg_classes classes
ON courses.class_id = classes.class_id
LEFT JOIN DATAFESTAFRICA.INTERMEDIATE.stg_teachers teachers
ON courses.teacher_id = teachers.teacher_id