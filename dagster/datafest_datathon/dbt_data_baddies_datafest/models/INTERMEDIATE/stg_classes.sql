SELECT DISTINCT
    id as class_id,
    school_id,
    short_name as class_short_name,
    title as class_title,
    sort_order,
    created_at as class_created_at,
    TO_CHAR(CAST(created_at AS DATE), 'YYYYMMDD') AS class_created_date_id,
    updated_at as class_updated_at,
    TO_CHAR(CAST(updated_at AS DATE), 'YYYYMMDD') AS class_updated_date_id
FROM {{ source('data_baddies_datafest', 'raw_school_gradelevels') }}
