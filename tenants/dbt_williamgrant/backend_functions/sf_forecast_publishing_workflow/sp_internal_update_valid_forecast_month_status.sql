-- This script creates the global platform status table and the internal helper
-- procedure to keep it updated in real-time based on publication events.


-- Step 1: Create the global state table
CREATE TABLE IF NOT EXISTS FORECAST.DEPLETIONS_FORECAST_PLATFORM_STATUS (
    ID INT NOT NULL DEFAULT 1,
    VALID_FORECAST_GENERATION_MONTH_DATE DATE,
    LAST_UPDATE_TIMESTAMP TIMESTAMP_NTZ,
    LAST_UPDATE_BY_PROCEDURE VARCHAR,
    CONSTRAINT PK_FORECAST_PLATFORM_STATUS PRIMARY KEY (ID)
);

-- Step 2: Seed the table with an initial value if it's empty
-- This ensures the GET UDF always has a value to return.
INSERT INTO FORECAST.DEPLETIONS_FORECAST_PLATFORM_STATUS (ID, VALID_FORECAST_GENERATION_MONTH_DATE, LAST_UPDATE_TIMESTAMP, LAST_UPDATE_BY_PROCEDURE)
SELECT 1, (SELECT MAX(dfid.FORECAST_GENERATION_MONTH_DATE) FROM FORECAST.DEPLETIONS_FORECAST_INIT_DRAFT AS dfid), CURRENT_TIMESTAMP(), 'INITIAL_SEED'
WHERE NOT EXISTS (SELECT 1 FROM FORECAST.DEPLETIONS_FORECAST_PLATFORM_STATUS WHERE ID = 1);


-- Step 3: Create the internal helper procedure to update the state
CREATE OR REPLACE PROCEDURE FORECAST._INTERNAL_SP_UPDATE_VALID_FORECAST_MONTH_STATUS(P_CALLING_PROCEDURE VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Internal helper procedure. Do not call directly. Calculates and updates the valid forecast generation month date in the global status table.'
AS
$$
DECLARE
    V_VALID_FGMD DATE;
BEGIN
    -- This block contains the optimized logic to determine the correct FGMD
    WITH base_dates AS (
        SELECT
            -- Optimized date selection using QUALIFY
            FORECAST_GENERATION_MONTH_DATE AS current_fgmd,
            DATEADD(MONTH, -1, FORECAST_GENERATION_MONTH_DATE)::DATE as previous_fgmd
        FROM FORECAST.DEPLETIONS_FORECAST_INIT_DRAFT
        QUALIFY ROW_NUMBER() OVER (ORDER BY IS_CURRENT_FORECAST_GENERATION DESC, FORECAST_GENERATION_MONTH_DATE DESC) = 1
    ),
    active_market_count AS (
        SELECT COUNT(DISTINCT mm.MARKET_ID) as market_count
        FROM MASTER_DATA.MARKETS_MASTER AS mm
        WHERE mm.MARKET_ID != 'USAUS1'
    ),
    consensus_market_count AS (
        -- Optimized query to count consensus markets directly
        SELECT COUNT(DISTINCT p.MARKET_CODE) as market_count
        FROM FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS p, base_dates bd
        WHERE p.PUBLICATION_STATUS = 'consensus'
          AND p.FORECAST_GENERATION_MONTH_DATE = bd.previous_fgmd
    )
    SELECT
        CASE
            WHEN cmc.market_count >= amc.market_count -- Use >= for safety
            THEN bd.current_fgmd
            ELSE bd.previous_fgmd
        END
    INTO :V_VALID_FGMD
    FROM
        base_dates bd,
        active_market_count amc,
        consensus_market_count cmc;

    -- Update the global state table with the newly calculated date
    UPDATE FORECAST.DEPLETIONS_FORECAST_PLATFORM_STATUS
    SET
        VALID_FORECAST_GENERATION_MONTH_DATE = :V_VALID_FGMD,
        LAST_UPDATE_TIMESTAMP = CURRENT_TIMESTAMP(),
        LAST_UPDATE_BY_PROCEDURE = :P_CALLING_PROCEDURE
    WHERE ID = 1;

    RETURN 'SUCCESS: Platform status updated.';
END;
$$; 