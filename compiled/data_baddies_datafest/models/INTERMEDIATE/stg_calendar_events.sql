SELECT DISTINCT
    id as calendar_event_id,
    CAST(syear AS INT) as session_year,
    school_id,
    school_date as event_date,
    title as event_name,
    description as event_description,
    created_at as event_created_at,
    TO_CHAR(CAST(created_at AS DATE), 'YYYYMMDD') AS event_created_date_id,
    updated_at as event_updated_at,
    TO_CHAR(CAST(updated_at AS DATE), 'YYYYMMDD') AS event_updated_date_id
FROM DATAFESTAFRICA.RAW.raw_calendar_events