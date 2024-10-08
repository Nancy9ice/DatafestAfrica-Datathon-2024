SELECT DISTINCT
    student_id,
    exam_year,
    jamb_score
FROM {{ source('data_baddies_datafest', 'raw_student_jamb_scores') }}
