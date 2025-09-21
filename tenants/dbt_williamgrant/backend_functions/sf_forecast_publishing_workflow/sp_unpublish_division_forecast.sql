-- Snowflake SQL Stored Procedure for unpublish_division_forecasts - OPTIMIZED V5 SQL
-- Target Schema: APOLLO_WILLIAMGRANT.FORECAST
-- Optimized for maximum performance using bulk operations (same architecture as JS/Python V5)


CREATE OR REPLACE PROCEDURE FORECAST.SP_UNPUBLISH_DIVISION_FORECAST(
    P_DIVISION VARCHAR,
    P_FORECAST_GENERATION_MONTH_DATE_STR VARCHAR,
    P_USER_ID VARCHAR,
    P_NOTE VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    -- Exceptions
    invalid_date_ex EXCEPTION (-20001, 'Invalid forecast generation month date format. Must be YYYY-MM-01.');
    no_publications_ex EXCEPTION (-20002, 'No active publications found for the specified division and forecast period.');

    -- Variables
    V_FGMD DATE;
    V_PUBLICATION_COUNT INTEGER := 0;
    V_MARKET_COUNT INTEGER := 0;
    V_NOTE_TO_USE VARCHAR;
    V_FUTURE_FGMD DATE;
    V_REVERT_COMMENT VARCHAR;
BEGIN
    -- ========= VALIDATION =========
    IF (P_FORECAST_GENERATION_MONTH_DATE_STR NOT RLIKE '^\\d{4}-\\d{2}-01$') THEN
        RAISE invalid_date_ex;
    END IF;
    
    V_FGMD := TO_DATE(P_FORECAST_GENERATION_MONTH_DATE_STR);
    V_NOTE_TO_USE := COALESCE(P_NOTE, 'Unpublished by ' || P_USER_ID || ' on ' || CURRENT_TIMESTAMP()::VARCHAR);
    V_FUTURE_FGMD := DATEADD(MONTH, 1, V_FGMD);
    V_REVERT_COMMENT := 'Sync: Reverted as ' || P_FORECAST_GENERATION_MONTH_DATE_STR || ' was unpublished.';

    -- Check if we have any publications to unpublish
    SELECT COUNT(DISTINCT p.PUBLICATION_ID), COUNT(DISTINCT p.MARKET_CODE)
    INTO :V_PUBLICATION_COUNT, :V_MARKET_COUNT
    FROM FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS p
    JOIN FORECAST.DEPLETIONS_FORECAST_PUBLICATION_GROUPS g ON p.GROUP_ID = g.GROUP_ID
    WHERE g.DIVISION = :P_DIVISION
      AND g.FORECAST_GENERATION_MONTH_DATE = :V_FGMD
      AND g.GROUP_STATUS = 'active'
      AND p.PUBLICATION_STATUS IN ('review', 'consensus');
    
    IF (V_PUBLICATION_COUNT = 0) THEN
        RETURN 'SUCCESS: No active publications found to unpublish for division ' || P_DIVISION;
    END IF;
    
    BEGIN TRANSACTION;
    
    BEGIN
        -- ========= BULK UNPUBLISH OPERATIONS =========
        
        -- Step 1: Bulk unpublish all publications for this division
        UPDATE FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS
        SET PUBLICATION_STATUS = 'unpublished',
            APPROVAL_STATUS_DATE = CURRENT_TIMESTAMP(),
            PUBLISHED_BY_USER_ID = :P_USER_ID,
            PUBLICATION_NOTE = :V_NOTE_TO_USE
        WHERE PUBLICATION_ID IN (
            SELECT p.PUBLICATION_ID
            FROM FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS p
            JOIN FORECAST.DEPLETIONS_FORECAST_PUBLICATION_GROUPS g ON p.GROUP_ID = g.GROUP_ID
            WHERE g.DIVISION = :P_DIVISION
              AND g.FORECAST_GENERATION_MONTH_DATE = :V_FGMD
              AND g.GROUP_STATUS = 'active'
              AND p.PUBLICATION_STATUS IN ('review', 'consensus')
        );
        
        -- Step 2: Bulk delete published forecasts
        DELETE FROM FORECAST.DEPLETIONS_FORECAST_PUBLISHED_FORECASTS
        WHERE PUBLICATION_ID IN (
            SELECT p.PUBLICATION_ID
            FROM FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS p
            JOIN FORECAST.DEPLETIONS_FORECAST_PUBLICATION_GROUPS g ON p.GROUP_ID = g.GROUP_ID
            WHERE g.DIVISION = :P_DIVISION
              AND g.FORECAST_GENERATION_MONTH_DATE = :V_FGMD
              AND p.PUBLICATION_STATUS = 'unpublished'
        );
        
        -- Step 2b: Bulk delete chains published forecasts
        DELETE FROM FORECAST.DEPLETIONS_FORECAST_PUBLISHED_FORECASTS_CHAINS
        WHERE PUBLICATION_ID IN (
            SELECT p.PUBLICATION_ID
            FROM FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS p
            JOIN FORECAST.DEPLETIONS_FORECAST_PUBLICATION_GROUPS g ON p.GROUP_ID = g.GROUP_ID
            WHERE g.DIVISION = :P_DIVISION
              AND g.FORECAST_GENERATION_MONTH_DATE = :V_FGMD
              AND p.PUBLICATION_STATUS = 'unpublished'
        );
        
        -- Step 3: Bulk update manual input statuses to draft
        UPDATE FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST
        SET FORECAST_STATUS = 'draft'
        WHERE MARKET_CODE IN (
            SELECT DISTINCT h.MARKET_CODE
            FROM MASTER_DATA.STG_HYPERION__DIVISION_HIERARCHY h
            WHERE h.DIVISION = :P_DIVISION
        )
        AND FORECAST_GENERATION_MONTH_DATE = :V_FGMD
        AND FORECAST_STATUS IN ('review', 'consensus');
        
        -- Step 4: Delete ALL records from future FGMD for this division (robust approach)
        -- Since the division is unpublished, remove all future FGMD data for all markets in this division
        DELETE FROM FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST_VERSIONS
        WHERE FORECAST_ID IN (
            SELECT m.ID
            FROM FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST m
            WHERE m.MARKET_CODE IN (
                SELECT DISTINCT h.MARKET_CODE
                FROM MASTER_DATA.STG_HYPERION__DIVISION_HIERARCHY h
                WHERE h.DIVISION = :P_DIVISION
            )
            AND m.FORECAST_GENERATION_MONTH_DATE = :V_FUTURE_FGMD
        );
        
        -- Step 5: Delete ALL forecast records from future FGMD for this division
        DELETE FROM FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST
        WHERE MARKET_CODE IN (
            SELECT DISTINCT h.MARKET_CODE
            FROM MASTER_DATA.STG_HYPERION__DIVISION_HIERARCHY h
            WHERE h.DIVISION = :P_DIVISION
        )
        AND FORECAST_GENERATION_MONTH_DATE = :V_FUTURE_FGMD;
        
        -- Step 6: Update group statuses to inactive if no active publications remain
        UPDATE FORECAST.DEPLETIONS_FORECAST_PUBLICATION_GROUPS g
        SET GROUP_STATUS = 'inactive'
        WHERE g.DIVISION = :P_DIVISION
          AND g.FORECAST_GENERATION_MONTH_DATE = :V_FGMD
          AND NOT EXISTS (
              SELECT 1
              FROM FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS p
              WHERE p.GROUP_ID = g.GROUP_ID
                AND p.PUBLICATION_STATUS IN ('review', 'consensus')
          );
        
        -- Step 7: Update global forecast status
        CALL FORECAST._INTERNAL_SP_UPDATE_VALID_FORECAST_MONTH_STATUS('SP_UNPUBLISH_DIVISION_FORECAST');
        
        COMMIT;
        RETURN 'SUCCESS: Unpublished division ' || P_DIVISION || ' - ' || V_PUBLICATION_COUNT || ' publications across ' || V_MARKET_COUNT || ' markets';
    EXCEPTION
        WHEN OTHER THEN
            ROLLBACK;
            RAISE; -- Re-raise to fail the procedure call
    END;

-- Remove EXCEPTION block entirely - let all exceptions bubble up unhandled
-- This makes SQL version fully consistent with Python/JavaScript versions and main publish procedure
END;
$$;

COMMENT ON PROCEDURE FORECAST.SP_UNPUBLISH_DIVISION_FORECAST(VARCHAR, VARCHAR, VARCHAR, VARCHAR) IS
'V6 WITH CHAINS: High-performance division unpublishing using bulk operations and set-based processing.
Eliminates chained procedure calls and row-by-row processing for maximum performance.
Same architecture as JavaScript/Python V5 versions.
CHAINS INTEGRATION: Automatically unpublishes both core and chains published forecasts.';

-- Example Usage:
-- CALL FORECAST.SP_UNPUBLISH_DIVISION_FORECAST('INDEPENDENT FRANCHISE', '2025-06-01', 'user_admin', 'Division forecasts retracted.'); 