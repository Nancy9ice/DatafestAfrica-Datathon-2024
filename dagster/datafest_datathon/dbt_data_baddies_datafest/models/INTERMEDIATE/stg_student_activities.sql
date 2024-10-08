SELECT DISTINCT
    student_id,
    activity_id,
    created_at,
    CASE 
        WHEN is_active = 1 THEN 'Active'
        WHEN is_active = 0 THEN 'Not active'
    END AS student_activity_status,
    TO_CHAR(CAST(created_at AS DATE), 'YYYYMMDD') AS created_date_id,
    updated_at,
    TO_CHAR(CAST(updated_at AS DATE), 'YYYYMMDD') AS updated_date_id
FROM {{ source('data_baddies_datafest', 'raw_eligibility') }}