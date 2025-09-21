{{ config(
    materialized = 'view',
    post_hook = "COMMENT ON VIEW {{ this }} IS
    'View-based replacement for GET_DIVISION_FORECAST_PUBLICATION_HISTORY UDTF.
    Retrieves forecast publication history. Use WHERE clauses to filter by:
    - division: WHERE division = ''YourDivision'' OR division IS NULL (for all)
    - include_unpublished: WHERE is_unpublished = FALSE (to exclude unpublished)
    - forecast_generation_month_date: WHERE forecast_generation_month_date = ''2025-01-01''
    Usage patterns:
    - All records: SELECT * FROM {{ this }};
    - Specific division: SELECT * FROM {{ this }} WHERE division = ''SomeDivision'';
    - Exclude unpublished: SELECT * FROM {{ this }} WHERE is_unpublished = FALSE;
    - Specific month: SELECT * FROM {{ this }} WHERE forecast_generation_month_date = ''2025-01-01'';'"
) }}

SELECT 
    p.PUBLICATION_ID,
    g.GROUP_ID,
    g.DIVISION,
    p.MARKET_CODE,
    g.PUBLICATION_DATE,
    p.PUBLISHED_BY_USER_ID,
    p.FORECAST_GENERATION_MONTH_DATE,
    p.PUBLICATION_NOTE,
    p.PUBLICATION_STATUS,
    p.APPROVAL_STATUS_DATE,
    COUNT(pf.ID)::INTEGER AS FORECAST_COUNT,
    
    -- Additional columns for enhanced filtering (not in original UDTF)
    CASE WHEN p.PUBLICATION_STATUS = 'unpublished' THEN TRUE ELSE FALSE END AS IS_UNPUBLISHED,
    CASE WHEN g.DIVISION IS NOT NULL THEN FALSE ELSE TRUE END AS IS_ACTIVE_DIVISION
FROM 
    {{ source('forecast', 'depletions_forecast_publication_groups') }} g
JOIN
    {{ source('forecast', 'depletions_forecast_publications') }} p ON g.GROUP_ID = p.GROUP_ID
LEFT JOIN
    {{ source('forecast', 'depletions_forecast_published_forecasts') }} pf ON p.PUBLICATION_ID = pf.PUBLICATION_ID
GROUP BY
    p.PUBLICATION_ID,
    g.GROUP_ID,
    g.DIVISION,
    p.MARKET_CODE,
    g.PUBLICATION_DATE,
    p.PUBLISHED_BY_USER_ID,
    p.FORECAST_GENERATION_MONTH_DATE,
    p.PUBLICATION_NOTE,
    p.PUBLICATION_STATUS,
    p.APPROVAL_STATUS_DATE