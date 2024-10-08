SELECT DISTINCT
    id as offence_id,
    student_id,
    school_id,
    staff_id as teacher_id,
    entry_date as offence_recorded_date,
    TO_CHAR(CAST(entry_date AS DATE), 'YYYYMMDD') AS offence_recorded_date_id,
    referral_date as disciplinary_date,
    TO_CHAR(CAST(referral_date AS DATE), 'YYYYMMDD') AS disciplinary_date_id,
    offence as student_offence,
    action_taken as disciplinary_action_taken
FROM {{ source('data_baddies_datafest', 'raw_discipline_referrals') }}

