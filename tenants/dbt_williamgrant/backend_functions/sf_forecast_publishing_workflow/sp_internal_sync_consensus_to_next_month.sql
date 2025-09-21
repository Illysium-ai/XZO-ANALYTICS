
CREATE OR REPLACE PROCEDURE FORECAST._INTERNAL_SP_SYNC_CONSENSUS_TO_NEXT_MONTH(
    P_CURRENT_FGMD DATE,
    P_DIVISIONS_CSV VARCHAR, -- Comma-separated list of divisions to sync
    P_USER_ID VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'OPTIMIZED V3: Internal helper to sync consensus forecasts to next month FGMD. MAJOR FIXES: (1) Proper transaction handling without nesting (2) Smart MERGE logic: only updates records that have actually changed (prevents duplicate version history) (3) Market unpublish scenario: Markets already in future FGMD wont get unnecessary updates/duplicates (4) Version history: uses MERGE on FORECAST_ID+VERSION_NUMBER (deterministic, no timing dependencies)'
AS
$$
DECLARE
    V_FUTURE_FGMD DATE;
    V_MERGE_COUNT INTEGER := 0;
    V_VERSION_COUNT INTEGER := 0;
BEGIN
    V_FUTURE_FGMD := DATEADD(MONTH, 1, P_CURRENT_FGMD);

    -- *** TRANSACTION HANDLING REMOVED ***
    -- The main procedure handles the transaction, so we don't start our own
    -- This fixes the nested transaction issue that was preventing sync

    MERGE INTO FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST AS target
    USING (
        -- Select all records that were just promoted to 'consensus' for the current month and division.
        SELECT 
            MARKET_NAME, MARKET_CODE, DISTRIBUTOR_NAME, DISTRIBUTOR_ID,
            BRAND, BRAND_ID, VARIANT, VARIANT_ID, VARIANT_SIZE_PACK_DESC, VARIANT_SIZE_PACK_ID,
            FORECAST_YEAR, MONTH, FORECAST_METHOD, MANUAL_CASE_EQUIVALENT_VOLUME, COMMENT
        FROM FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST
        WHERE FORECAST_STATUS = 'consensus'
          AND FORECAST_GENERATION_MONTH_DATE = :P_CURRENT_FGMD
          AND FORECAST_YEAR = DATE_PART('year', :V_FUTURE_FGMD)
          AND MONTH > DATE_PART('month', :V_FUTURE_FGMD)
          AND MARKET_CODE IN (
              SELECT DISTINCT MARKET_CODE FROM MASTER_DATA.STG_HYPERION__DIVISION_HIERARCHY 
              WHERE DIVISION IN (SELECT TRIM(VALUE) FROM TABLE(SPLIT_TO_TABLE(:P_DIVISIONS_CSV, ',')))
          )
    ) AS source
    ON (
        target.MARKET_CODE = source.MARKET_CODE
        AND target.DISTRIBUTOR_ID = source.DISTRIBUTOR_ID
        AND target.VARIANT_SIZE_PACK_ID = source.VARIANT_SIZE_PACK_ID
        AND target.FORECAST_YEAR = source.FORECAST_YEAR
        AND target.MONTH = source.MONTH
        AND target.FORECAST_GENERATION_MONTH_DATE = :V_FUTURE_FGMD
    )
    WHEN MATCHED AND (
        target.MANUAL_CASE_EQUIVALENT_VOLUME != source.MANUAL_CASE_EQUIVALENT_VOLUME OR
        COALESCE(target.COMMENT, '') != COALESCE(source.COMMENT, '')
    ) THEN
        UPDATE SET
            target.MANUAL_CASE_EQUIVALENT_VOLUME = source.MANUAL_CASE_EQUIVALENT_VOLUME,
            target.FORECAST_STATUS = 'draft', -- Always reset status to draft for the new month
            target.UPDATED_BY_USER_ID = :P_USER_ID,
            target.COMMENT = source.COMMENT,
            target.UPDATED_AT = CURRENT_TIMESTAMP(),
            target.CURRENT_VERSION = target.CURRENT_VERSION + 1, -- Only increment if actually changed
            target.FORECAST_METHOD = source.FORECAST_METHOD
    WHEN NOT MATCHED THEN
        INSERT (
            MARKET_NAME, MARKET_CODE, DISTRIBUTOR_NAME, DISTRIBUTOR_ID,
            BRAND, BRAND_ID, VARIANT, VARIANT_ID, VARIANT_SIZE_PACK_DESC, VARIANT_SIZE_PACK_ID,
            FORECAST_YEAR, MONTH, FORECAST_METHOD, FORECAST_GENERATION_MONTH_DATE,
            MANUAL_CASE_EQUIVALENT_VOLUME, UPDATED_BY_USER_ID, FORECAST_STATUS, COMMENT,
            UPDATED_AT, CURRENT_VERSION
        ) VALUES (
            source.MARKET_NAME, source.MARKET_CODE, source.DISTRIBUTOR_NAME, source.DISTRIBUTOR_ID,
            source.BRAND, source.BRAND_ID, source.VARIANT, source.VARIANT_ID, source.VARIANT_SIZE_PACK_DESC, source.VARIANT_SIZE_PACK_ID,
            source.FORECAST_YEAR, source.MONTH, source.FORECAST_METHOD, :V_FUTURE_FGMD,
            source.MANUAL_CASE_EQUIVALENT_VOLUME, :P_USER_ID, 'draft', source.COMMENT,
            CURRENT_TIMESTAMP(), 1
        );
    
    V_MERGE_COUNT := SQLROWCOUNT;

    -- Use MERGE to ensure version history exists for all synced records (no duplicates, no timing dependencies)
    MERGE INTO FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST_VERSIONS AS target
    USING (
        SELECT 
            m.ID as FORECAST_ID, 
            m.CURRENT_VERSION as VERSION_NUMBER,
            m.MARKET_NAME, m.MARKET_CODE,
            m.DISTRIBUTOR_NAME, m.DISTRIBUTOR_ID, 
            m.BRAND, m.BRAND_ID, m.VARIANT, m.VARIANT_ID,
            m.VARIANT_SIZE_PACK_DESC, m.VARIANT_SIZE_PACK_ID, 
            m.FORECAST_YEAR, m.MONTH, m.FORECAST_METHOD,
            m.FORECAST_GENERATION_MONTH_DATE, m.MANUAL_CASE_EQUIVALENT_VOLUME,
            m.UPDATED_BY_USER_ID, m.FORECAST_STATUS, m.COMMENT
        FROM FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST m
        WHERE m.FORECAST_GENERATION_MONTH_DATE = :V_FUTURE_FGMD
        AND m.UPDATED_BY_USER_ID = :P_USER_ID
        AND m.FORECAST_YEAR = DATE_PART('year', :V_FUTURE_FGMD)
        AND m.MONTH > DATE_PART('month', :V_FUTURE_FGMD)
        AND m.MARKET_CODE IN (
            SELECT DISTINCT MARKET_CODE FROM MASTER_DATA.STG_HYPERION__DIVISION_HIERARCHY 
            WHERE DIVISION IN (SELECT TRIM(VALUE) FROM TABLE(SPLIT_TO_TABLE(:P_DIVISIONS_CSV, ',')))
        )
    ) AS source
    ON (target.FORECAST_ID = source.FORECAST_ID AND target.VERSION_NUMBER = source.VERSION_NUMBER)
    WHEN NOT MATCHED THEN
        INSERT (
            FORECAST_ID, VERSION_NUMBER, MARKET_NAME, MARKET_CODE,
            DISTRIBUTOR_NAME, DISTRIBUTOR_ID, BRAND, BRAND_ID, VARIANT, VARIANT_ID,
            VARIANT_SIZE_PACK_DESC, VARIANT_SIZE_PACK_ID, FORECAST_YEAR, MONTH, FORECAST_METHOD,
            FORECAST_GENERATION_MONTH_DATE, MANUAL_CASE_EQUIVALENT_VOLUME,
            UPDATED_BY_USER_ID, FORECAST_STATUS, COMMENT, CREATED_AT
        ) VALUES (
            source.FORECAST_ID, source.VERSION_NUMBER, source.MARKET_NAME, source.MARKET_CODE,
            source.DISTRIBUTOR_NAME, source.DISTRIBUTOR_ID, source.BRAND, source.BRAND_ID, source.VARIANT, source.VARIANT_ID,
            source.VARIANT_SIZE_PACK_DESC, source.VARIANT_SIZE_PACK_ID, source.FORECAST_YEAR, source.MONTH, source.FORECAST_METHOD,
            source.FORECAST_GENERATION_MONTH_DATE, source.MANUAL_CASE_EQUIVALENT_VOLUME,
            source.UPDATED_BY_USER_ID, source.FORECAST_STATUS, source.COMMENT, CURRENT_TIMESTAMP()
        );

    V_VERSION_COUNT := SQLROWCOUNT;

    -- *** TRANSACTION HANDLING REMOVED ***
    -- No COMMIT here - let the main procedure handle it
    -- This ensures atomicity across the entire operation

    RETURN 'SUCCESS: Synced ' || V_MERGE_COUNT || ' records to ' || V_FUTURE_FGMD || '. Created ' || V_VERSION_COUNT || ' version history entries.';

-- *** ERROR HANDLING IMPROVED ***
-- Remove the EXCEPTION block entirely to let errors bubble up to the main procedure
-- This ensures the main procedure sees sync failures and can rollback properly
END;
$$; 