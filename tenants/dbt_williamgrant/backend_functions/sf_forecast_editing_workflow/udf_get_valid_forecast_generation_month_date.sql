-- Snowflake SQL UDF for get_valid_forecast_generation_month_date
-- Target Schema: APOLLO_WILLIAMGRANT.FORECAST


CREATE OR REPLACE FUNCTION FORECAST.UDF_GET_VALID_FORECAST_GENERATION_MONTH_DATE()
RETURNS DATE
LANGUAGE SQL
MEMOIZABLE
COMMENT = 'Determines the valid forecast generation month date by reading from the FORECAST_PLATFORM_STATUS table. The state in this table is updated in real-time by the publishing and unpublishing stored procedures.'
AS
$$
  -- This function now reads from the pre-calculated global state table for maximum performance.
  -- The state is updated by the various promotion/unpublish stored procedures.
  SELECT VALID_FORECAST_GENERATION_MONTH_DATE
  FROM FORECAST.DEPLETIONS_FORECAST_PLATFORM_STATUS
  WHERE ID = 1
$$;

-- Example Usage:
-- SELECT FORECAST.UDF_GET_VALID_FORECAST_GENERATION_MONTH_DATE(); 