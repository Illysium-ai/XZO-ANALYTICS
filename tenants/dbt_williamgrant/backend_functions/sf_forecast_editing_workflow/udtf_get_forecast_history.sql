-- Snowflake User-Defined Table Function (UDTF) for get_forecast_history
-- Target Schema: APOLLO_WILLIAMGRANT.FORECAST


CREATE OR REPLACE FUNCTION FORECAST.UDTF_GET_FORECAST_HISTORY (P_FORECAST_ID INTEGER)
RETURNS TABLE (
    VERSION_NUMBER INTEGER,
    FORECAST_STATUS VARCHAR(50), -- Mapped from 'state'
    MARKET_NAME VARCHAR(100),
    BRAND VARCHAR(100),
    FORECAST_YEAR INTEGER,
    MONTH INTEGER,
    MANUAL_CASE_EQUIVALENT_VOLUME FLOAT, -- Changed from NUMERIC
    UPDATED_BY_USER_ID VARCHAR(50),
    CREATED_AT TIMESTAMP_NTZ, -- Changed from TIMESTAMP
    COMMENT VARCHAR -- Changed from TEXT
)
AS
$$
    SELECT 
        fv.VERSION_NUMBER,
        fv.FORECAST_STATUS, -- Corresponds to 'state' in the original PG function output
        fv.MARKET_NAME,
        fv.BRAND,
        fv.FORECAST_YEAR,
        fv.MONTH,
        fv.MANUAL_CASE_EQUIVALENT_VOLUME,
        fv.UPDATED_BY_USER_ID,
        fv.CREATED_AT,
        fv.COMMENT
    FROM 
        FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST_VERSIONS fv
    WHERE 
        fv.FORECAST_ID = P_FORECAST_ID
    ORDER BY 
        fv.VERSION_NUMBER DESC
$$;

COMMENT ON FUNCTION FORECAST.UDTF_GET_FORECAST_HISTORY (INTEGER) IS 
  'Retrieves the version history for a given forecast_id from the manual_input_depletions_forecast_versions table.';

-- Example Usage (after creating the function and having data):
-- SELECT * FROM TABLE(FORECAST.UDTF_GET_FORECAST_HISTORY(your_forecast_id_value)); 