SELECT DISTINCT
    marking_period_id,
    mp_source as source,
    school_id,
    mp_type as term_type,
    title as assessment_type,
    sort_order,
    points as overall_score
FROM {{ source('data_baddies_datafest', 'raw_marking_periods') }}