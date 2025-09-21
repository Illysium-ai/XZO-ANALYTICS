-- Snowflake SQL Stored Procedure for unpublish_group - OPTIMIZED V2 SQL
-- Target Schema: APOLLO_WILLIAMGRANT.FORECAST
-- Optimized for maximum performance using bulk operations (same architecture as division/market V2 SQL)


CREATE OR REPLACE PROCEDURE FORECAST.SP_UNPUBLISH_GROUP(
    P_GROUP_ID INTEGER,
    P_USER_ID VARCHAR,
    P_NOTE VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    -- Exceptions
    group_not_found_ex EXCEPTION (-20001, 'Publication group ID not found.');
    no_publications_ex EXCEPTION (-20002, 'No active publications found for the specified group.');

    -- Variables
    V_GROUP_EXISTS BOOLEAN;
    V_PUBLICATION_COUNT INTEGER := 0;
    V_MARKET_COUNT INTEGER := 0;
    V_NOTE_TO_USE VARCHAR;
    V_FGMD DATE;
    V_FUTURE_FGMD DATE;
    V_REVERT_COMMENT VARCHAR;
    V_DIVISION VARCHAR;
BEGIN
    -- ========= VALIDATION =========
    -- Check if group exists and get group details
    SELECT g.FORECAST_GENERATION_MONTH_DATE, g.DIVISION
    INTO :V_FGMD, :V_DIVISION
    FROM FORECAST.DEPLETIONS_FORECAST_PUBLICATION_GROUPS g
    WHERE g.GROUP_ID = :P_GROUP_ID;
    
    -- Set group exists flag based on whether we found the group
    V_GROUP_EXISTS := (V_FGMD IS NOT NULL);
    
    IF (NOT V_GROUP_EXISTS) THEN
        RAISE group_not_found_ex;
    END IF;
    
    V_NOTE_TO_USE := COALESCE(P_NOTE, 'Unpublished by ' || P_USER_ID || ' on ' || CURRENT_TIMESTAMP()::VARCHAR);
    V_FUTURE_FGMD := DATEADD(MONTH, 1, V_FGMD);
    V_REVERT_COMMENT := 'Sync: Reverted as ' || V_FGMD::VARCHAR || ' was unpublished.';

    -- Check if we have any publications to unpublish in this group
    SELECT COUNT(p.PUBLICATION_ID), COUNT(DISTINCT p.MARKET_CODE)
    INTO :V_PUBLICATION_COUNT, :V_MARKET_COUNT
    FROM FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS p
    WHERE p.GROUP_ID = :P_GROUP_ID
      AND p.PUBLICATION_STATUS IN ('review', 'consensus');
    
    IF (V_PUBLICATION_COUNT = 0) THEN
        RETURN 'SUCCESS: No active publications found to unpublish for group ' || P_GROUP_ID;
    END IF;
    
    BEGIN TRANSACTION;
    
    BEGIN
        -- ========= BULK UNPUBLISH OPERATIONS =========
        
        -- Step 1: Bulk unpublish all publications in this group
        UPDATE FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS
        SET PUBLICATION_STATUS = 'unpublished',
            APPROVAL_STATUS_DATE = CURRENT_TIMESTAMP(),
            PUBLISHED_BY_USER_ID = :P_USER_ID,
            PUBLICATION_NOTE = :V_NOTE_TO_USE
        WHERE GROUP_ID = :P_GROUP_ID
          AND PUBLICATION_STATUS IN ('review', 'consensus');
        
        -- Step 2: Bulk delete published forecasts for this group
        DELETE FROM FORECAST.DEPLETIONS_FORECAST_PUBLISHED_FORECASTS
        WHERE PUBLICATION_ID IN (
            SELECT p.PUBLICATION_ID
            FROM FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS p
            WHERE p.GROUP_ID = :P_GROUP_ID
              AND p.PUBLICATION_STATUS = 'unpublished'
        );
        
        -- Step 3: Bulk update manual input statuses to draft for all markets in this group
        UPDATE FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST
        SET FORECAST_STATUS = 'draft'
        WHERE MARKET_CODE IN (
            SELECT DISTINCT p.MARKET_CODE
            FROM FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS p
            WHERE p.GROUP_ID = :P_GROUP_ID
        )
        AND FORECAST_GENERATION_MONTH_DATE = :V_FGMD
        AND FORECAST_STATUS IN ('review', 'consensus');
        
        -- Step 4: Delete ALL records from future FGMD for this group (robust approach)
        -- Since the group is unpublished, remove all future FGMD data for all markets in this group
        DELETE FROM FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST_VERSIONS
        WHERE FORECAST_ID IN (
            SELECT m.ID
            FROM FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST m
            WHERE m.MARKET_CODE IN (
                SELECT DISTINCT p.MARKET_CODE
                FROM FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS p
                WHERE p.GROUP_ID = :P_GROUP_ID
            )
            AND m.FORECAST_GENERATION_MONTH_DATE = :V_FUTURE_FGMD
        );
        
        -- Step 5: Delete ALL forecast records from future FGMD for this group
        DELETE FROM FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST
        WHERE MARKET_CODE IN (
            SELECT DISTINCT p.MARKET_CODE
            FROM FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS p
            WHERE p.GROUP_ID = :P_GROUP_ID
        )
        AND FORECAST_GENERATION_MONTH_DATE = :V_FUTURE_FGMD;
        
        -- Step 6: Update group status to inactive
        UPDATE FORECAST.DEPLETIONS_FORECAST_PUBLICATION_GROUPS g
        SET GROUP_STATUS = 'inactive'
        WHERE g.GROUP_ID = :P_GROUP_ID;
        
        -- Step 7: Update global forecast status
        CALL FORECAST._INTERNAL_SP_UPDATE_VALID_FORECAST_MONTH_STATUS('SP_UNPUBLISH_GROUP');
        
        COMMIT;
        RETURN 'SUCCESS: Unpublished group ' || P_GROUP_ID || ' (' || V_DIVISION || ') - ' || V_PUBLICATION_COUNT || ' publications across ' || V_MARKET_COUNT || ' markets';
    EXCEPTION
        WHEN OTHER THEN
            ROLLBACK;
            RAISE; -- Re-raise to fail the procedure call
    END;

-- Remove EXCEPTION block entirely - let all exceptions bubble up unhandled
-- This makes SQL version fully consistent with Python/JavaScript versions and main publish procedure
END;
$$;

COMMENT ON PROCEDURE FORECAST.SP_UNPUBLISH_GROUP(INTEGER, VARCHAR, VARCHAR) IS
'V2 SQL OPTIMIZED: High-performance group unpublishing using bulk operations and set-based processing.
Eliminates chained procedure calls and row-by-row processing for maximum performance.
Same architecture as division/market V2 SQL versions.';

-- Example Usage:
-- CALL FORECAST.SP_UNPUBLISH_GROUP(123, 'user_admin', 'Group unpublished due to data refresh.'); 