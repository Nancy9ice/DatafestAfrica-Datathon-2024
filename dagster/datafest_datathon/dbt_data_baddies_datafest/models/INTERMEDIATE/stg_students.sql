SELECT DISTINCT
    student_id,
    parent_id,
    first_name,
    last_name,
    middle_name,
    CONCAT(first_name, ' ', middle_name, ' ', last_name) AS full_name,
    grade_level_id as class_id,
    CASE WHEN bus_pickup = 'Y' THEN 'Yes'
        ELSE 'No'
    END AS bus_pickup,
    CASE WHEN bus_dropoff = 'Y' THEN 'Yes'
        ELSE 'No'
    END AS bus_dropoff,
    CASE WHEN departments IS NULL THEN 'None'
        ELSE departments
    END AS department,
    CASE WHEN gender = 'M' THEN 'Male'
        WHEN gender = 'F' THEN 'Female'
    END AS gender,
    CASE 
        WHEN health_status = 1 THEN 'Very Good Condition'
        WHEN health_status = 2 THEN 'Good Condition'
        WHEN health_status = 3 THEN 'Average Condition'
        WHEN health_status = 4 THEN 'Poor Condition'
    END AS health_condition,
    created_at as student_registered_at,
    TO_CHAR(CAST(created_at AS DATE), 'YYYYMMDD') AS registration_date_id,
    updated_at as student_updated_at,
    TO_CHAR(CAST(updated_at AS DATE), 'YYYYMMDD') AS updated_date_id
FROM {{ source('data_baddies_datafest', 'raw_students') }}