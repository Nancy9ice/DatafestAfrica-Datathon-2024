SELECT DISTINCT
    course_id,
    subject_id,
    school_id,
    grade_level as class_id,
    teacher_id,
    title as course_title,
    short_name as course_short_name,
    CASE 
        WHEN department IS NULL THEN 'General'
    ELSE department
    END AS course_department,
    (credit_hours * 16 * 60) as total_teaching_hours_period,
    description as course_description,
    created_at as course_created_at,
    TO_CHAR(CAST(created_at AS DATE), 'YYYYMMDD') AS course_created_date_id,
    updated_at as course_updated_at,
    TO_CHAR(CAST(updated_at AS DATE), 'YYYYMMDD') AS course_updated_date_id
FROM datafestafrica.RAW.raw_courses