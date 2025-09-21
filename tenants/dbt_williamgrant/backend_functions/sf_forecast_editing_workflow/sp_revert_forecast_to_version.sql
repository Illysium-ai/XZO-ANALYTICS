-- Snowflake Stored Procedure for revert_forecast_to_version
-- Target Schema: APOLLO_WILLIAMGRANT.FORECAST


CREATE OR REPLACE PROCEDURE FORECAST.SP_REVERT_FORECAST_TO_VERSION(
    P_FORECAST_IDS ARRAY,
    P_VERSION_NUMBER INTEGER,  -- Use version 0 to revert to original forecast
    P_USER_ID VARCHAR,
    P_COMMENT VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    -- Exception
    forecast_published_ex EXCEPTION (-20081, 'Cannot revert one or more forecasts; at least one forecast belongs to a published market.');
    
    -- Variables
    V_PROCESSED_COUNT INTEGER := 0;
    V_IS_PUBLISHED_FLAG BOOLEAN;
BEGIN
    -- Pre-flight Check: Ensure no forecast in the batch belongs to a published market.
    SELECT COUNT(*) > 0 INTO :V_IS_PUBLISHED_FLAG
    FROM FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST f
    WHERE f.ID IN (SELECT VALUE::INTEGER FROM TABLE(FLATTEN(INPUT => :P_FORECAST_IDS)))
      AND FORECAST.UDF_IS_DEPLETIONS_FORECAST_PUBLISHED(f.MARKET_CODE, f.FORECAST_GENERATION_MONTH_DATE);
      
    IF (V_IS_PUBLISHED_FLAG) THEN
        RAISE forecast_published_ex;
    END IF;

    BEGIN TRANSACTION;

    BEGIN
        IF (P_VERSION_NUMBER = 0) THEN
            -- Action: Soft-delete the manual inputs, reverting to trend-driven forecast.
            -- This is a single, atomic update.
            UPDATE FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST
            SET 
                FORECAST_STATUS = 'REVERTED_TO_TREND', 
                COMMENT = COALESCE(:P_COMMENT, 'Reverted to trend') || ' by user ' || :P_USER_ID, 
                UPDATED_BY_USER_ID = :P_USER_ID, 
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE ID IN (SELECT VALUE::INTEGER FROM TABLE(FLATTEN(INPUT => :P_FORECAST_IDS)));
            V_PROCESSED_COUNT := SQLROWCOUNT;

            -- Note: The original logic created a version history entry for this.
            -- In this simplified model, a "revert to trend" is treated as a status change
            -- rather than a new version, simplifying the logic. The history of the
            -- manual input is still preserved in the versions table from previous saves.

        ELSE
            -- Action: Revert specified forecasts to a specific previous version.
            -- This is more complex and best handled in a loop.
            FOR rec IN (
                SELECT
                    v.FORECAST_ID,
                    v.MARKET_NAME, v.MARKET_CODE, v.DISTRIBUTOR_NAME, v.DISTRIBUTOR_ID,
                    v.BRAND, v.BRAND_ID, v.VARIANT, v.VARIANT_ID, v.VARIANT_SIZE_PACK_DESC, v.VARIANT_SIZE_PACK_ID,
                    v.FORECAST_YEAR, v.MONTH, v.FORECAST_METHOD, v.FORECAST_GENERATION_MONTH_DATE,
                    v.MANUAL_CASE_EQUIVALENT_VOLUME, v.FORECAST_STATUS
                FROM FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST_VERSIONS v
                WHERE v.VERSION_NUMBER = :P_VERSION_NUMBER
                  AND EXISTS (SELECT 1 FROM TABLE(FLATTEN(INPUT => :P_FORECAST_IDS)) f WHERE f.VALUE::INTEGER = v.FORECAST_ID)
            )
            DO
                CALL FORECAST.SP_SAVE_FORECAST_VERSION(
                    rec.FORECAST_ID, rec.MARKET_NAME, rec.MARKET_CODE, rec.DISTRIBUTOR_NAME, 
                    rec.DISTRIBUTOR_ID, rec.BRAND, rec.BRAND_ID, rec.VARIANT, rec.VARIANT_ID,
                    rec.VARIANT_SIZE_PACK_DESC, rec.VARIANT_SIZE_PACK_ID, rec.FORECAST_YEAR, rec.MONTH, rec.FORECAST_METHOD, 
                    rec.FORECAST_GENERATION_MONTH_DATE, rec.MANUAL_CASE_EQUIVALENT_VOLUME, :P_USER_ID, rec.FORECAST_STATUS, 
                    'Reverted to version ' || :P_VERSION_NUMBER || '. ' || COALESCE(:P_COMMENT, '')
                );
                V_PROCESSED_COUNT := V_PROCESSED_COUNT + 1;
            END FOR;
        END IF;
        
        COMMIT;
        RETURN 'SUCCESS: Reverted ' || V_PROCESSED_COUNT || ' forecast(s).';
    EXCEPTION
        WHEN OTHER THEN
            ROLLBACK;
            RAISE; -- Re-raise to fail the procedure call
    END;

-- Remove EXCEPTION block entirely - let all exceptions bubble up unhandled
-- This makes SQL version fully consistent with Python/JavaScript versions and main publish procedure
END;
$$;

COMMENT ON PROCEDURE FORECAST.SP_REVERT_FORECAST_TO_VERSION(ARRAY, INTEGER, VARCHAR, VARCHAR) IS
'Reverts one or more forecasts to a specified version or to trend.
If P_VERSION_NUMBER is 0, reverts to trend (updates status to REVERTED_TO_TREND).
If P_VERSION_NUMBER > 0, reverts to that specific historical version by calling SAVE_FORECAST_VERSION.
Depends on SAVE_FORECAST_VERSION.';

-- Example Usage:
-- CALL FORECAST.SP_REVERT_FORECAST_TO_VERSION(ARRAY_CONSTRUCT(1, 2, 3), 0, 'user123', 'Reverting to trend per manager request.');
-- CALL FORECAST.SP_REVERT_FORECAST_TO_VERSION(ARRAY_CONSTRUCT(4), 2, 'user123', 'Reverting item 4 to its version 2.'); 