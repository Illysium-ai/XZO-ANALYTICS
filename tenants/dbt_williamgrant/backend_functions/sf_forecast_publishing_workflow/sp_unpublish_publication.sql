-- Snowflake SQL Stored Procedure for unpublish_publication - OPTIMIZED V2 SQL
-- Target Schema: APOLLO_WILLIAMGRANT.FORECAST
-- Optimized for maximum performance using bulk operations (same architecture as division/market V2 SQL)


CREATE OR REPLACE PROCEDURE FORECAST.SP_UNPUBLISH_PUBLICATION(
    P_PUBLICATION_ID INTEGER,
    P_USER_ID VARCHAR,
    P_NOTE VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    -- Exceptions
    publication_not_found_ex EXCEPTION (-20001, 'Publication ID not found.');
    already_unpublished_ex EXCEPTION (-20002, 'Publication is already unpublished.');

    -- Variables
    V_PUBLICATION_MARKET_CODE VARCHAR;
    V_PUBLICATION_FGMD DATE;
    V_PUBLICATION_STATUS_ORIGINAL VARCHAR;
    V_GROUP_ID INTEGER;
    V_NOTE_TO_USE VARCHAR;
    V_FUTURE_FGMD DATE;
    V_REVERT_COMMENT VARCHAR;
BEGIN
    -- ========= VALIDATION =========
    SELECT p.MARKET_CODE, p.FORECAST_GENERATION_MONTH_DATE, p.PUBLICATION_STATUS, p.GROUP_ID
    INTO :V_PUBLICATION_MARKET_CODE, :V_PUBLICATION_FGMD, :V_PUBLICATION_STATUS_ORIGINAL, :V_GROUP_ID
    FROM FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS p
    WHERE p.PUBLICATION_ID = :P_PUBLICATION_ID;
    
    IF (V_PUBLICATION_MARKET_CODE IS NULL) THEN
        RAISE publication_not_found_ex;
    END IF;
    
    IF (V_PUBLICATION_STATUS_ORIGINAL = 'unpublished') THEN
        RAISE already_unpublished_ex;
    END IF;
    
    V_NOTE_TO_USE := COALESCE(P_NOTE, 'Unpublished by ' || P_USER_ID || ' on ' || CURRENT_TIMESTAMP()::VARCHAR);
    V_FUTURE_FGMD := DATEADD(MONTH, 1, V_PUBLICATION_FGMD);
    V_REVERT_COMMENT := 'Sync: Reverted as ' || V_PUBLICATION_FGMD::VARCHAR || ' was unpublished.';
    
    BEGIN TRANSACTION;
    
    BEGIN
        -- ========= BULK UNPUBLISH OPERATIONS =========
        
        -- Step 1: Unpublish the specific publication
        UPDATE FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS
        SET PUBLICATION_STATUS = 'unpublished',
            APPROVAL_STATUS_DATE = CURRENT_TIMESTAMP(),
            PUBLISHED_BY_USER_ID = :P_USER_ID,
            PUBLICATION_NOTE = :V_NOTE_TO_USE
        WHERE PUBLICATION_ID = :P_PUBLICATION_ID;
        
        -- Step 2: Delete published forecasts for this publication
        DELETE FROM FORECAST.DEPLETIONS_FORECAST_PUBLISHED_FORECASTS
        WHERE PUBLICATION_ID = :P_PUBLICATION_ID;
        
        -- Step 3: Update manual input status to draft for this market
        UPDATE FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST
        SET FORECAST_STATUS = 'draft'
        WHERE MARKET_CODE = :V_PUBLICATION_MARKET_CODE
          AND FORECAST_GENERATION_MONTH_DATE = :V_PUBLICATION_FGMD
          AND FORECAST_STATUS IN ('review', 'consensus');
        
        -- Step 4: Delete ALL records from future FGMD for this market (robust approach)
        -- Since the publication is unpublished, remove all future FGMD data for this market
        DELETE FROM FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST_VERSIONS
        WHERE FORECAST_ID IN (
            SELECT m.ID
            FROM FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST m
            WHERE m.MARKET_CODE = :V_PUBLICATION_MARKET_CODE
            AND m.FORECAST_GENERATION_MONTH_DATE = :V_FUTURE_FGMD
        );
        
        -- Step 5: Delete ALL forecast records from future FGMD for this market
        DELETE FROM FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST
        WHERE MARKET_CODE = :V_PUBLICATION_MARKET_CODE
          AND FORECAST_GENERATION_MONTH_DATE = :V_FUTURE_FGMD;
        
        -- Step 6: Update group status to inactive if no active publications remain
        UPDATE FORECAST.DEPLETIONS_FORECAST_PUBLICATION_GROUPS g
        SET GROUP_STATUS = 'inactive'
        WHERE g.GROUP_ID = :V_GROUP_ID
          AND NOT EXISTS (
              SELECT 1
              FROM FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS p
              WHERE p.GROUP_ID = g.GROUP_ID
                AND p.PUBLICATION_STATUS IN ('review', 'consensus')
          );
        
        -- Step 7: Update global forecast status
        CALL FORECAST._INTERNAL_SP_UPDATE_VALID_FORECAST_MONTH_STATUS('SP_UNPUBLISH_PUBLICATION');
        
        COMMIT;
        RETURN 'SUCCESS: Unpublished publication ' || P_PUBLICATION_ID || ' for market ' || V_PUBLICATION_MARKET_CODE;
    EXCEPTION
        WHEN OTHER THEN
            ROLLBACK;
            RAISE; -- Re-raise to fail the procedure call
    END;

-- Remove EXCEPTION block entirely - let all exceptions bubble up unhandled
-- This makes SQL version fully consistent with Python/JavaScript versions and main publish procedure
END;
$$;

COMMENT ON PROCEDURE FORECAST.SP_UNPUBLISH_PUBLICATION(INTEGER, VARCHAR, VARCHAR) IS
'V2 SQL OPTIMIZED: High-performance publication unpublishing using bulk operations and set-based processing.
Eliminates row-by-row processing for maximum performance. Same architecture as division/market V2 SQL versions.
Returns VARCHAR status message instead of BOOLEAN for consistency with other V2 procedures.';

-- Example Usage:
-- CALL FORECAST.SP_UNPUBLISH_PUBLICATION(123, 'user_admin', 'Data correction needed.'); 