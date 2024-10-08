

WITH total_scores as (
    SELECT 
        student_course_id,
        term,
        SUM(total_scores) as total_scores
    FROM DATAFESTAFRICA.CORE.fact_school_grades
    GROUP BY student_course_id, term
),

school_grades AS (
SELECT 
    sc.student_id,
    ROUND(AVG(ts.total_scores), 1) AS average_student_score  -- Calculate average scores across terms
FROM 
    total_scores AS ts
LEFT JOIN 
    DATAFESTAFRICA.INTERMEDIATE.stg_students_courses AS sc 
ON 
    ts.student_course_id = sc.student_course_id
GROUP BY 
    sc.student_id-- Group by student_id and course_id to get the average score
ORDER BY student_id
),

aggregated_attendance AS (
    SELECT 
        sc.student_id,
        AVG(att.minutes_present) AS avg_student_minutes_present,
        AVG(att.total_minutes) AS avg_student_total_minutes
    FROM 
        DATAFESTAFRICA.INTERMEDIATE.stg_attendance att
    LEFT JOIN 
        DATAFESTAFRICA.INTERMEDIATE.stg_students_courses sc
    ON 
        att.student_course_id = sc.student_course_id
    GROUP BY sc.student_id
)

SELECT DISTINCT
    students.student_id,
    students.student_class,
    students.student_status,
    department,
    gender,
    parents.full_name AS student_parent,
    parents.education_level AS parent_education,
    student_activity_status,
    student_extracurricular_activity,
    CAST(agg_attendance.avg_student_minutes_present AS INT) AS average_student_minutes_attendance,
    CAST(agg_attendance.avg_student_total_minutes AS INT) AS average_expected_student_attendance,
    sg.average_student_score,
    CASE 
        WHEN average_student_score >= 50 THEN 'Pass'
        WHEN average_student_score < 50 THEN 'Fail'
    END AS student_school_performance,
    CASE 
        WHEN discipline.student_offence IS NULL THEN 'None'
        ELSE discipline.student_offence
    END AS student_offence,
    CASE 
        WHEN disciplinary_action_taken IS NULL THEN 'None'
        ELSE disciplinary_action_taken
    END AS disciplinary_action_taken,
    CASE 
        WHEN teacher.full_name IS NULL THEN 'None'
        ELSE teacher.full_name
    END AS disciplinary_teacher,
    jamb.exam_year AS jamb_exam_year,
    jamb.jamb_score,
    -- The school's threshold for JAMB performance is 200
    CASE 
        WHEN jamb.jamb_score >= 200 THEN 'Pass'
        WHEN jamb.jamb_score < 200 THEN 'Fail'
    END AS jamb_performance
FROM DATAFESTAFRICA.CORE.dim_students students
LEFT JOIN DATAFESTAFRICA.INTERMEDIATE.stg_parents parents
    ON students.parent_id = parents.parent_id
LEFT JOIN DATAFESTAFRICA.INTERMEDIATE.stg_students_courses students_courses
    ON students_courses.student_id = students.student_id
LEFT JOIN aggregated_attendance agg_attendance
    ON agg_attendance.student_id = students.student_id
LEFT JOIN school_grades sg
    ON sg.student_id = students.student_id
LEFT JOIN DATAFESTAFRICA.INTERMEDIATE.stg_jamb_scores jamb
    ON jamb.student_id = students.student_id
LEFT JOIN DATAFESTAFRICA.INTERMEDIATE.stg_disciplinary_records discipline
    ON discipline.student_id = students.student_id
LEFT JOIN DATAFESTAFRICA.INTERMEDIATE.stg_teachers teacher
    ON discipline.teacher_id = teacher.teacher_id