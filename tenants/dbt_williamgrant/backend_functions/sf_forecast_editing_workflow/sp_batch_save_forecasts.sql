
CREATE OR REPLACE PROCEDURE FORECAST.SP_BATCH_SAVE_FORECASTS(
    P_FORECASTS_JSON VARCHAR, -- JSON array as a string
    P_FORECAST_GENERATION_MONTH_DATE DATE,
    P_USER_ID VARCHAR,
    P_FORECAST_STATUS VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    -- Exceptions
    forecast_published_ex EXCEPTION (-20091, 'Cannot save batch; at least one forecast belongs to a published market.');
    duplicate_forecast_ex EXCEPTION (-20092, 'Duplicate forecast data found in JSON for records with volume. Each full forecast record must be unique.');
    missing_required_fields_ex EXCEPTION (-20093, 'Missing required field. Please provide market_code and forecast_method in each record.');
    missing_volume_fields_ex EXCEPTION (-20094, 'Missing required fields for volume update. Please provide variant_size_pack_id, forecast_year, and month when manual_case_equivalent_volume is present.');

    -- Variables
    V_UPDATE_COUNT INTEGER := 0;
    V_MERGE_COUNT INTEGER := 0;
    V_LOCKED_MARKETS ARRAY;
    V_TEMP_COUNT INTEGER;
BEGIN
    -- Pre-flight Check 1: Find any markets in the payload that are already published.
    SELECT ARRAY_AGG(DISTINCT s.market_code) INTO :V_LOCKED_MARKETS
    FROM (
        SELECT f.value:market_code::VARCHAR AS market_code
        FROM TABLE(FLATTEN(INPUT => PARSE_JSON(:P_FORECASTS_JSON))) f
    ) s
    WHERE FORECAST.UDF_IS_DEPLETIONS_FORECAST_PUBLISHED(s.market_code, :P_FORECAST_GENERATION_MONTH_DATE);
    
    IF (ARRAY_SIZE(:V_LOCKED_MARKETS) > 0) THEN
        RAISE forecast_published_ex;
    END IF;

    -- Pre-flight Check 2: Check for duplicate forecast records with volume in the JSON input.
    SELECT COUNT(*) INTO :V_TEMP_COUNT FROM (
        SELECT 1
        FROM TABLE(FLATTEN(INPUT => PARSE_JSON(:P_FORECASTS_JSON))) f
        WHERE f.value:manual_case_equivalent_volume IS NOT NULL
        GROUP BY
            f.value:market_code::VARCHAR,
            COALESCE(f.value:customer_id::VARCHAR, 'N/A'),
            f.value:variant_size_pack_id::VARCHAR,
            f.value:forecast_year::INTEGER,
            f.value:month::INTEGER
        HAVING COUNT(*) > 1
    );
    IF (V_TEMP_COUNT > 0) THEN
        RAISE duplicate_forecast_ex;
    END IF;

    -- Pre-flight Check 3: Validate that minimally required fields are present.
    SELECT COUNT(*) INTO :V_TEMP_COUNT
    FROM TABLE(FLATTEN(INPUT => PARSE_JSON(:P_FORECASTS_JSON))) f
    WHERE (f.value:market_code::VARCHAR IS NULL OR f.value:market_code::VARCHAR = '')
       OR (f.value:forecast_method::VARCHAR IS NULL OR TRIM(f.value:forecast_method::VARCHAR) = '');
    IF (V_TEMP_COUNT > 0) THEN
       RAISE missing_required_fields_ex;
    END IF;

    -- Pre-flight Check 4: Validate fields required when volume is present.
    SELECT COUNT(*) INTO :V_TEMP_COUNT
    FROM TABLE(FLATTEN(INPUT => PARSE_JSON(:P_FORECASTS_JSON))) f
    WHERE f.value:manual_case_equivalent_volume IS NOT NULL
      AND (
           (f.value:variant_size_pack_id::VARCHAR IS NULL OR f.value:variant_size_pack_id::VARCHAR = '')
        OR (f.value:forecast_year::INTEGER IS NULL)
        OR (f.value:month::INTEGER IS NULL)
      );
    IF (V_TEMP_COUNT > 0) THEN
        RAISE missing_volume_fields_ex;
    END IF;

    BEGIN TRANSACTION;

    BEGIN
        -- Step 1: Use a single MERGE statement with CTEs to handle all volume updates
        MERGE INTO FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST AS target
        USING (
            WITH unnested_forecasts AS (
                SELECT
                    f.value:market_code::VARCHAR AS market_code,
                    f.value:customer_id::VARCHAR AS customer_id,
                    f.value:variant_size_pack_id::VARCHAR AS variant_size_pack_id,
                    f.value:forecast_year::INTEGER AS forecast_year,
                    f.value:month::INTEGER AS month,
                    f.value:forecast_method::VARCHAR AS forecast_method,
                    f.value:manual_case_equivalent_volume::FLOAT AS agg_volume,
                    f.value:brand::VARCHAR AS brand,
                    f.value:brand_id::VARCHAR AS brand_id,
                    f.value:variant::VARCHAR AS variant,
                    f.value:variant_id::VARCHAR AS variant_id,
                    f.value:variant_size_pack_desc::VARCHAR AS variant_size_pack_desc,
                    f.value:comment::TEXT AS comment
                FROM
                    TABLE(FLATTEN(input => PARSE_JSON(:P_FORECASTS_JSON))) f
                WHERE
                    f.value:manual_case_equivalent_volume IS NOT NULL
            )
            SELECT
                d.market_name,
                uf.market_code,
                d.distributor_name,
                d.distributor_id,
                uf.brand,
                uf.brand_id,
                uf.variant,
                uf.variant_id,
                uf.variant_size_pack_desc,
                uf.variant_size_pack_id,
                uf.forecast_year,
                uf.month,
                uf.forecast_method,
                (uf.agg_volume * CASE WHEN COALESCE(uf.customer_id, '') = '' THEN d.distributor_allocation ELSE COALESCE(d.customer_allocation, d.distributor_allocation) END)::FLOAT AS allocated_volume,
                uf.comment
            FROM unnested_forecasts uf
            JOIN FORECAST.DISTRIBUTOR_ALLOCATION_BY_MARKET_SIZE_PACK d
                ON uf.market_code = d.market_code
                AND uf.variant_size_pack_id = d.variant_size_pack_id
                AND (uf.customer_id IS NULL OR uf.customer_id = d.customer_id)
        ) AS source
        ON (
            target.MARKET_CODE = source.market_code
            AND target.DISTRIBUTOR_ID = source.distributor_id
            AND target.VARIANT_SIZE_PACK_ID = source.variant_size_pack_id
            AND target.FORECAST_YEAR = source.forecast_year
            AND target.MONTH = source.month
            AND target.FORECAST_GENERATION_MONTH_DATE = :P_FORECAST_GENERATION_MONTH_DATE
        )
        WHEN MATCHED THEN
            UPDATE SET
                target.MANUAL_CASE_EQUIVALENT_VOLUME = source.allocated_volume,
                target.UPDATED_BY_USER_ID = :P_USER_ID,
                target.FORECAST_STATUS = :P_FORECAST_STATUS,
                target.COMMENT = source.comment,
                target.UPDATED_AT = CURRENT_TIMESTAMP(),
                target.CURRENT_VERSION = target.CURRENT_VERSION + 1,
                target.FORECAST_METHOD = source.forecast_method
        WHEN NOT MATCHED THEN
            INSERT (
                MARKET_NAME, MARKET_CODE, DISTRIBUTOR_NAME, DISTRIBUTOR_ID,
                BRAND, BRAND_ID, VARIANT, VARIANT_ID, VARIANT_SIZE_PACK_DESC, VARIANT_SIZE_PACK_ID,
                FORECAST_YEAR, MONTH, FORECAST_METHOD, FORECAST_GENERATION_MONTH_DATE,
                MANUAL_CASE_EQUIVALENT_VOLUME, UPDATED_BY_USER_ID, FORECAST_STATUS, COMMENT,
                UPDATED_AT, CURRENT_VERSION
            ) VALUES (
                source.market_name, source.market_code, source.distributor_name, source.distributor_id,
                source.brand, source.brand_id, source.variant, source.variant_id, source.variant_size_pack_desc, source.variant_size_pack_id,
                source.forecast_year, source.month, source.forecast_method, :P_FORECAST_GENERATION_MONTH_DATE,
                source.allocated_volume, :P_USER_ID, :P_FORECAST_STATUS, source.comment,
                CURRENT_TIMESTAMP(), 1
            );

        V_MERGE_COUNT := SQLROWCOUNT;

        -- Step 2: Create new versions for all records that were just updated or inserted by the MERGE.
        -- This single, set-based INSERT is more robust and performant than a loop.
        INSERT INTO FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST_VERSIONS (
            FORECAST_ID, VERSION_NUMBER, MARKET_NAME, MARKET_CODE, DISTRIBUTOR_NAME, DISTRIBUTOR_ID,
            BRAND, BRAND_ID, VARIANT, VARIANT_ID, VARIANT_SIZE_PACK_DESC, VARIANT_SIZE_PACK_ID, FORECAST_YEAR,
            MONTH, FORECAST_METHOD, FORECAST_GENERATION_MONTH_DATE, MANUAL_CASE_EQUIVALENT_VOLUME,
            UPDATED_BY_USER_ID, FORECAST_STATUS, COMMENT
        )
        SELECT
            t.ID,
            t.CURRENT_VERSION, -- Log the new current version number
            t.MARKET_NAME, t.MARKET_CODE, t.DISTRIBUTOR_NAME, t.DISTRIBUTOR_ID, t.BRAND, t.BRAND_ID,
            t.VARIANT, t.VARIANT_ID, t.VARIANT_SIZE_PACK_DESC, t.VARIANT_SIZE_PACK_ID, t.FORECAST_YEAR,
            t.MONTH, t.FORECAST_METHOD, t.FORECAST_GENERATION_MONTH_DATE, t.MANUAL_CASE_EQUIVALENT_VOLUME,
            t.UPDATED_BY_USER_ID, t.FORECAST_STATUS, t.COMMENT
        FROM FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST t
        JOIN (
            -- Re-unnest the JSON to identify all records that were part of this batch operation.
            SELECT 
                f.value:market_code::VARCHAR AS market_code,
                f.value:customer_id::VARCHAR AS customer_id,
                f.value:variant_size_pack_id::VARCHAR AS variant_size_pack_id,
                f.value:forecast_year::INTEGER AS forecast_year,
                f.value:month::INTEGER AS month
            FROM TABLE(FLATTEN(INPUT => PARSE_JSON(:P_FORECASTS_JSON))) f
            WHERE f.value:manual_case_equivalent_volume IS NOT NULL
        ) AS s 
        ON t.MARKET_CODE = s.market_code 
           AND (s.customer_id IS NULL OR LEFT(t.DISTRIBUTOR_ID, 5) = s.customer_id)
           AND t.VARIANT_SIZE_PACK_ID = s.variant_size_pack_id
           AND t.FORECAST_YEAR = s.forecast_year
           AND t.MONTH = s.month
        WHERE t.FORECAST_GENERATION_MONTH_DATE = :P_FORECAST_GENERATION_MONTH_DATE
          AND NOT EXISTS ( -- Only insert a version if it doesn't already exist for that number
              SELECT 1 FROM FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST_VERSIONS v
              WHERE v.FORECAST_ID = t.ID AND v.VERSION_NUMBER = t.CURRENT_VERSION
          );

        -- Step 3: Update the primary forecast method for all records in the batch.
        -- This ensures the selected forecast method is saved, regardless of whether a volume was also submitted.
        UPDATE FORECAST.DEPLETIONS_FORECAST_PRIMARY_FORECAST_METHOD AS target
        SET target.FORECAST_METHOD = f.value:forecast_method::VARCHAR
        FROM
            TABLE(FLATTEN(input => PARSE_JSON(:P_FORECASTS_JSON))) f
        WHERE
            target.MARKET_CODE = f.value:market_code::VARCHAR
            AND target.VARIANT_SIZE_PACK_ID = f.value:variant_size_pack_id::VARCHAR
            AND target.FORECAST_GENERATION_MONTH_DATE = :P_FORECAST_GENERATION_MONTH_DATE
            AND (f.value:customer_id IS NULL OR LEFT(target.DISTRIBUTOR_ID, 5) = f.value:customer_id::VARCHAR);
        
        V_UPDATE_COUNT := SQLROWCOUNT;

        COMMIT;
        
        RETURN 'SUCCESS: Processed batch. Primary methods updated: ' || V_UPDATE_COUNT || '. Volume records merged: ' || V_MERGE_COUNT || '.';
    EXCEPTION
        WHEN OTHER THEN
            ROLLBACK;
            RAISE; -- Re-raise to fail the procedure call
    END;

-- Remove EXCEPTION block entirely - let all exceptions bubble up unhandled
-- This makes SQL version fully consistent with Python/JavaScript versions and main publish procedure
END;
$$; 