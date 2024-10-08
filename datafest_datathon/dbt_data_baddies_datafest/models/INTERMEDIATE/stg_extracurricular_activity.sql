SELECT DISTINCT
    id as activity_id,
    school_id,
    title as activity,
    comment as activity_description,
    created_at as activity_created_at,
    TO_CHAR(CAST(created_at AS DATE), 'YYYYMMDD') AS activity_created_date_id,
    updated_at as activity_updated_at,
    TO_CHAR(CAST(updated_at AS DATE), 'YYYYMMDD') AS activity_updated_date_id
FROM {{ source('data_baddies_datafest', 'raw_eligibility_activities') }}