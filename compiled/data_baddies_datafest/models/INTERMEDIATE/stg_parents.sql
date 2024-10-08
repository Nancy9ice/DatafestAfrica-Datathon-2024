SELECT DISTINCT
    parent_id,
    last_name,
    first_name,
    middle_name,
    CONCAT(last_name, ' ', middle_name, ' ', first_name) AS full_name,
    education_level,
    created_at as parent_registered_at,
    TO_CHAR(CAST(created_at AS DATE), 'YYYYMMDD') AS registration_date_id,
    updated_at as parent_updated_at,
    TO_CHAR(CAST(updated_at AS DATE), 'YYYYMMDD') AS updated_date_id
FROM datafestafrica.RAW.raw_parent