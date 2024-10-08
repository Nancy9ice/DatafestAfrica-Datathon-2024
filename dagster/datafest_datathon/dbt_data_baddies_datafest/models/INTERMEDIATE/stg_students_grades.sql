SELECT DISTINCT
    student_course_id,
    marking_period_id,
    points as scores
FROM {{ source('data_baddies_datafest', 'raw_gradebook_grades') }}