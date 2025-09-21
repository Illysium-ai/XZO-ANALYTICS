-- Snowflake SQL UDF for is_depletions_forecast_published
-- Target Schema: APOLLO_WILLIAMGRANT.FORECAST


CREATE OR REPLACE FUNCTION FORECAST.UDF_IS_DEPLETIONS_FORECAST_PUBLISHED(
    P_MARKET_CODE VARCHAR,
    P_FORECAST_GENERATION_MONTH_DATE DATE
)
RETURNS BOOLEAN
LANGUAGE SQL
COMMENT = 'Checks if a forecast for a given market and forecast generation month date has been published (status is ''review'' or ''consensus''). Returns TRUE if published, otherwise FALSE.'
AS
$$
    SELECT COUNT(*) > 0
    FROM FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS p
    WHERE p.MARKET_CODE = P_MARKET_CODE
      AND p.FORECAST_GENERATION_MONTH_DATE = P_FORECAST_GENERATION_MONTH_DATE
      AND p.PUBLICATION_STATUS IN ('review', 'consensus')
$$;

COMMENT ON FUNCTION FORECAST.UDF_IS_DEPLETIONS_FORECAST_PUBLISHED(VARCHAR, DATE) IS
'Checks if a forecast for a given market and forecast generation month date has been published (status ''review'' or ''consensus'').
Returns a BOOLEAN value: TRUE if any matching publication is found, otherwise FALSE.';

-- Example Usage:
-- SELECT FORECAST.UDF_IS_DEPLETIONS_FORECAST_PUBLISHED('USACA1', '2025-01-01');

-- Example Usage:
-- SELECT FORECAST.UDF_IS_DEPLETIONS_FORECAST_PUBLISHED('USACA1', '2025-01-01');
-- SELECT 
--   result_object:is_published AS is_published,
--   result_object:publication_status AS status
-- FROM TABLE(SELECT FORECAST.UDF_IS_DEPLETIONS_FORECAST_PUBLISHED('USACA1', '2025-01-01') AS result_object); 