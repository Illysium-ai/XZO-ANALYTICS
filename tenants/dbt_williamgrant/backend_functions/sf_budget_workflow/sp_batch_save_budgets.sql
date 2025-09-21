CREATE OR REPLACE PROCEDURE FORECAST.SP_BATCH_SAVE_BUDGETS(
    P_BUDGETS_JSON VARCHAR, -- JSON array as a string
    P_BUDGET_CYCLE_DATE DATE,
    P_USER_ID VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    budget_approved_ex EXCEPTION (-20011, 'Cannot save batch; the budget cycle is approved (locked).');
    duplicate_budget_ex EXCEPTION (-20012, 'Duplicate budget data found in JSON for records with volume. Each record must be unique at the monthly grain.');
    missing_required_fields_ex EXCEPTION (-20013, 'Missing required field. Please provide market_code and a valid forecast_method in each record (allowed: three_month, six_month, twelve_month, flat, run_rate).');
    missing_volume_fields_ex EXCEPTION (-20014, 'Missing required fields for volume update. Provide variant_size_pack_id, forecast_year, and month when manual_case_equivalent_volume is present.');

    V_TEMP_COUNT INTEGER;
    V_BUDGETS VARIANT;
BEGIN
    -- Lock Check: Cycle approved?
    IF (FORECAST.UDF_IS_BUDGET_APPROVED(:P_BUDGET_CYCLE_DATE)) THEN
        RAISE budget_approved_ex;
    END IF;

    -- Parse input JSON once for reuse
    V_BUDGETS := PARSE_JSON(:P_BUDGETS_JSON);

    -- Pre-flight Check 1: Check for duplicates at method-agnostic monthly grain
    SELECT COUNT(*) INTO :V_TEMP_COUNT FROM (
        SELECT 1
        FROM TABLE(FLATTEN(INPUT => :V_BUDGETS)) f
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
        RAISE duplicate_budget_ex;
    END IF;

    -- Pre-flight Check 2: Validate minimally required fields
    SELECT COUNT(*) INTO :V_TEMP_COUNT
    FROM TABLE(FLATTEN(INPUT => :V_BUDGETS)) f
    WHERE (f.value:market_code::VARCHAR IS NULL OR f.value:market_code::VARCHAR = '')
       OR (f.value:forecast_method::VARCHAR IS NULL OR TRIM(f.value:forecast_method::VARCHAR) = '')
       OR (LOWER(f.value:forecast_method::VARCHAR) NOT IN ('three_month','six_month','twelve_month','flat','run_rate'));
    IF (V_TEMP_COUNT > 0) THEN
       RAISE missing_required_fields_ex;
    END IF;

    -- Pre-flight Check 3: Validate fields required when volume is present
    SELECT COUNT(*) INTO :V_TEMP_COUNT
    FROM TABLE(FLATTEN(INPUT => :V_BUDGETS)) f
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
        -- Manual volume upsert (method-agnostic) as before
        MERGE INTO FORECAST.MANUAL_INPUT_DEPLETIONS_BUDGET AS target
        USING (
            WITH unnested_budgets AS (
                SELECT
                    f.value:market_code::VARCHAR AS market_code,
                    f.value:customer_id::VARCHAR AS customer_id,
                    f.value:variant_size_pack_id::VARCHAR AS variant_size_pack_id,
                    f.value:forecast_year::INTEGER AS forecast_year,
                    f.value:month::INTEGER AS month,
                    f.value:manual_case_equivalent_volume::FLOAT AS agg_volume,
                    f.value:forecast_method::VARCHAR AS forecast_method,
                    f.value:comment::TEXT AS comment
                FROM TABLE(FLATTEN(input => :V_BUDGETS)) f
                WHERE f.value:manual_case_equivalent_volume IS NOT NULL
            ),
            sku_dedupe AS (
                SELECT DISTINCT
                    variant_size_pack_id,
                    brand,
                    brand_id,
                    variant,
                    variant_id,
                    variant_size_pack_desc
                FROM MASTER_DATA.APOLLO_SKU_MASTER
            )
            SELECT
                d.market_name,
                ub.market_code,
                d.distributor_name,
                d.distributor_id,
                sku.brand,
                sku.brand_id,
                sku.variant,
                sku.variant_id,
                sku.variant_size_pack_desc,
                ub.variant_size_pack_id,
                ub.forecast_year,
                ub.month,
                (ub.agg_volume * CASE WHEN COALESCE(ub.customer_id, '') = '' THEN d.distributor_allocation ELSE COALESCE(d.customer_allocation, d.distributor_allocation) END)::FLOAT AS allocated_volume,
                ub.comment,
                ub.forecast_method
            FROM unnested_budgets ub
            JOIN FORECAST.DISTRIBUTOR_ALLOCATION_BY_MARKET_SIZE_PACK d
              ON ub.market_code = d.market_code
             AND ub.variant_size_pack_id = d.variant_size_pack_id
             AND (ub.customer_id IS NULL OR ub.customer_id = d.customer_id)
            LEFT JOIN sku_dedupe sku
              ON sku.variant_size_pack_id = ub.variant_size_pack_id
        ) AS source
        ON (
            target.MARKET_CODE = source.market_code
            AND target.DISTRIBUTOR_ID = source.distributor_id
            AND target.VARIANT_SIZE_PACK_ID = source.variant_size_pack_id
            AND target.FORECAST_YEAR = source.forecast_year
            AND target.MONTH = source.month
            AND target.BUDGET_CYCLE_DATE = :P_BUDGET_CYCLE_DATE
        )
        WHEN MATCHED THEN
            UPDATE SET
                target.MANUAL_CASE_EQUIVALENT_VOLUME = source.allocated_volume,
                target.UPDATED_BY_USER_ID = :P_USER_ID,
                target.COMMENT = source.comment,
                target.UPDATED_AT = CURRENT_TIMESTAMP(),
                target.CURRENT_VERSION = target.CURRENT_VERSION + 1
        WHEN NOT MATCHED THEN
            INSERT (
                MARKET_NAME, MARKET_CODE, DISTRIBUTOR_NAME, DISTRIBUTOR_ID,
                BRAND, BRAND_ID, VARIANT, VARIANT_ID, VARIANT_SIZE_PACK_DESC, VARIANT_SIZE_PACK_ID,
                FORECAST_YEAR, MONTH, BUDGET_CYCLE_DATE,
                MANUAL_CASE_EQUIVALENT_VOLUME, UPDATED_BY_USER_ID, COMMENT,
                UPDATED_AT, CURRENT_VERSION
            ) VALUES (
                source.market_name, source.market_code, source.distributor_name, source.distributor_id,
                source.brand, source.brand_id, source.variant, source.variant_id, source.variant_size_pack_desc, source.variant_size_pack_id,
                source.forecast_year, source.month, :P_BUDGET_CYCLE_DATE,
                source.allocated_volume, :P_USER_ID, source.comment,
                CURRENT_TIMESTAMP(), 1
            );

        -- Versions for manual volume rows touched
        INSERT INTO FORECAST.MANUAL_INPUT_DEPLETIONS_BUDGET_VERSIONS (
            BUDGET_ID, VERSION_NUMBER, MARKET_NAME, MARKET_CODE, DISTRIBUTOR_NAME, DISTRIBUTOR_ID,
            BRAND, BRAND_ID, VARIANT, VARIANT_ID, VARIANT_SIZE_PACK_DESC, VARIANT_SIZE_PACK_ID, FORECAST_YEAR,
            MONTH, BUDGET_CYCLE_DATE, MANUAL_CASE_EQUIVALENT_VOLUME, UPDATED_BY_USER_ID, COMMENT
        )
        WITH touched_keys AS (
            SELECT DISTINCT
                f.value:market_code::VARCHAR AS market_code,
                f.value:customer_id::VARCHAR AS customer_id,
                f.value:variant_size_pack_id::VARCHAR AS variant_size_pack_id,
                f.value:forecast_year::INTEGER AS forecast_year,
                f.value:month::INTEGER AS month
            FROM TABLE(FLATTEN(INPUT => :V_BUDGETS)) f
            WHERE f.value:manual_case_equivalent_volume IS NOT NULL
        ),
        touched_distributors AS (
            SELECT DISTINCT
                tk.market_code,
                tk.variant_size_pack_id,
                tk.forecast_year,
                tk.month,
                d.distributor_id
            FROM touched_keys tk
            JOIN FORECAST.DISTRIBUTOR_ALLOCATION_BY_MARKET_SIZE_PACK d
              ON tk.market_code = d.market_code
             AND tk.variant_size_pack_id = d.variant_size_pack_id
             AND (tk.customer_id IS NULL OR tk.customer_id = d.customer_id)
        )
        SELECT
            t.ID,
            t.CURRENT_VERSION,
            t.MARKET_NAME, t.MARKET_CODE, t.DISTRIBUTOR_NAME, t.DISTRIBUTOR_ID, t.BRAND, t.BRAND_ID,
            t.VARIANT, t.VARIANT_ID, t.VARIANT_SIZE_PACK_DESC, t.VARIANT_SIZE_PACK_ID, t.FORECAST_YEAR,
            t.MONTH, t.BUDGET_CYCLE_DATE, t.MANUAL_CASE_EQUIVALENT_VOLUME, t.UPDATED_BY_USER_ID, t.COMMENT
        FROM FORECAST.MANUAL_INPUT_DEPLETIONS_BUDGET t
        JOIN touched_distributors td
          ON t.MARKET_CODE = td.market_code
         AND t.VARIANT_SIZE_PACK_ID = td.variant_size_pack_id
         AND t.FORECAST_YEAR = td.forecast_year
         AND t.MONTH = td.month
         AND t.DISTRIBUTOR_ID = td.distributor_id
         AND t.BUDGET_CYCLE_DATE = :P_BUDGET_CYCLE_DATE;

        -- Primary method update if provided in any record
        UPDATE FORECAST.DEPLETIONS_BUDGET_PRIMARY_METHOD AS target
        SET target.FORECAST_METHOD = f.value:forecast_method::VARCHAR,
            target.UPDATED_BY_USER_ID = :P_USER_ID,
            target.UPDATED_AT = CURRENT_TIMESTAMP()
        FROM
            TABLE(FLATTEN(input => :V_BUDGETS)) f
        WHERE
            target.BUDGET_CYCLE_DATE = :P_BUDGET_CYCLE_DATE
            AND target.MARKET_CODE = f.value:market_code::VARCHAR
            AND target.VARIANT_SIZE_PACK_ID = f.value:variant_size_pack_id::VARCHAR
            AND (f.value:customer_id IS NULL OR LEFT(target.DISTRIBUTOR_ID, 5) = f.value:customer_id::VARCHAR)
            AND f.value:forecast_method::VARCHAR IS NOT NULL
            AND TRIM(f.value:forecast_method::VARCHAR) != '';
        
        
        COMMIT;
        RETURN 'SUCCESS: Processed budget batch.';
    EXCEPTION
        WHEN OTHER THEN
            ROLLBACK;
            RAISE;
    END;
END;
$$;
