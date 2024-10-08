SELECT DISTINCT
    student_course_id,
    total_minutes,
    minutes_present,
    created_at as attendance_created_at,
    TO_CHAR(CAST(created_at AS DATE), 'YYYYMMDD') AS attendance_created_date_id,
    updated_at as attendance_updated_at,
    TO_CHAR(CAST(updated_at AS DATE), 'YYYYMMDD') AS attendance_updated_date_id
FROM datafestafrica.RAW.raw_attendance_day