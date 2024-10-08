SELECT DISTINCT 
    id as teacher_id,
    last_name,
    first_name,
    middle_name,
    CONCAT(first_name, ' ', middle_name, ' ', last_name) AS full_name,
    created_at as employment_date,
    TO_CHAR(CAST(created_at AS DATE), 'YYYYMMDD') AS employment_date_id,
    updated_at as updated_employment_date,
    TO_CHAR(CAST(updated_at AS DATE), 'YYYYMMDD') AS updated_employment_date_id
FROM datafestafrica.RAW.raw_teachers