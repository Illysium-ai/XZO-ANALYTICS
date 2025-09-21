-- Target Schema: APOLLO_WILLIAMGRANT.MASTER_DATA
-- Processes a batch of product tag updates directly (no nested procedure calls).
--
-- REFACTORING SUMMARY:
-- 1. Used parameterized cursor with bind variables and USING clause (proper Snowflake pattern)
-- 2. Cursor definition uses ? placeholder, parameter bound with OPEN cursor USING (variable)
-- 3. Explicit OPEN/FETCH/CLOSE cursor management for parameterized cursors
-- 4. Maintained all original business logic and exception handling
-- 5. This follows official Snowflake documentation for parameterized cursors


CREATE OR REPLACE PROCEDURE MASTER_DATA.SP_BATCH_UPDATE_APOLLO_VARIANT_SIZE_PACK_TAGS(
    P_TAG_DATA_STR VARCHAR -- JSON array as a string
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    -- Exceptions
    invalid_json_ex EXCEPTION (-20081, 'Invalid JSON format in input string.');
    processing_failed_ex EXCEPTION (-20082, 'Tag processing failed for one or more records. See error details.');
    vsp_not_found_ex EXCEPTION (-20051, 'Variant Size Pack ID not found in master data.');
    missing_desc_for_custom_ex EXCEPTION (-20083, 'variant_size_pack_desc is required when creating a custom product.');
    system_sku_cannot_be_custom_ex EXCEPTION (-20084, 'System SKU cannot be marked as custom.');
    invalid_is_planned_value_ex EXCEPTION (-20085, 'is_planned must be one of: true, false, default.');

    -- Parameterized cursor using bind variable placeholder
    -- This cursor extracts each JSON object from the input array
    json_records_cursor CURSOR FOR (
        SELECT VALUE AS record_data 
        FROM TABLE(FLATTEN(INPUT => PARSE_JSON(?)))
    );

    -- Variables for processing loop
    V_VARIANT_SIZE_PACK_ID VARCHAR;
    V_TAG_NAMES_ARRAY ARRAY;
    V_SUCCESS_COUNT INTEGER := 0;
    V_ERROR_COUNT INTEGER := 0;
    V_RECORD_DATA VARIANT;

    -- Variables for inline single-item logic (previously in SP_ADD_PRODUCT_TAGS_TO_VARIANT_SIZE_PACK)
    V_PROCESSED_TAG_IDS ARRAY;
    V_PROCESSED_TAG_NAMES ARRAY;
    V_EXISTS_IN_TAG_TABLE BOOLEAN;
    V_VSP_DESC_FROM_MASTER VARCHAR;
    V_INPUT_DESC VARCHAR;
    V_NEW_TAG_COUNT INTEGER;

    -- New fields
    V_IS_CUSTOM_PRODUCT BOOLEAN;
    V_IS_PLANNED_STR VARCHAR;
    V_IS_PLANNED_KEY_PROVIDED BOOLEAN;
    V_MARKET_CODE_EXCLUSIONS ARRAY;
    V_CUSTOMER_ID_EXCLUSIONS ARRAY;
    V_EXISTS_IN_SKU_MASTER BOOLEAN;
    V_TAG_NAMES_PRESENT_AND_ARRAY BOOLEAN;

    -- Aggregate list of VSPs that require realtime sync (do once on success)
    V_IDS_TO_SYNC ARRAY := ARRAY_CONSTRUCT();
BEGIN
    -- Validate JSON input before processing
    IF (P_TAG_DATA_STR IS NULL OR P_TAG_DATA_STR = '') THEN
        RAISE invalid_json_ex;
    END IF;

    -- Early JSON validation to provide a cleaner error than STATEMENT_ERROR
    IF (TRY_PARSE_JSON(P_TAG_DATA_STR) IS NULL) THEN
        RAISE invalid_json_ex;
    END IF;

    -- Open cursor with parameter binding using USING clause
    OPEN json_records_cursor USING (P_TAG_DATA_STR);

    -- Begin a single transaction for the entire batch (all-or-nothing)
    BEGIN TRANSACTION;

    -- Process each record using explicit cursor management
    LOOP
        -- Fetch next record from cursor
        FETCH json_records_cursor INTO V_RECORD_DATA;
        
        -- Exit loop when no more records
        IF (V_RECORD_DATA IS NULL) THEN
            EXIT;
        END IF;

        -- Reset variables per iteration
        V_VARIANT_SIZE_PACK_ID := NULL;
        V_TAG_NAMES_ARRAY := NULL;
        V_PROCESSED_TAG_IDS := ARRAY_CONSTRUCT();
        V_PROCESSED_TAG_NAMES := ARRAY_CONSTRUCT();
        V_EXISTS_IN_TAG_TABLE := FALSE;
        V_VSP_DESC_FROM_MASTER := NULL;
        V_INPUT_DESC := NULL;
        V_NEW_TAG_COUNT := NULL;
        V_IS_CUSTOM_PRODUCT := NULL;
        V_IS_PLANNED_STR := NULL;
        V_IS_PLANNED_KEY_PROVIDED := FALSE;
        V_MARKET_CODE_EXCLUSIONS := NULL;
        V_CUSTOMER_ID_EXCLUSIONS := NULL;
        
        -- Extract fields
        V_VARIANT_SIZE_PACK_ID := V_RECORD_DATA:variant_size_pack_id::VARCHAR;
        V_INPUT_DESC := V_RECORD_DATA:variant_size_pack_desc::VARCHAR;
        V_IS_CUSTOM_PRODUCT := COALESCE(V_RECORD_DATA:is_custom_product::BOOLEAN, NULL);
        -- Validate is_planned only if provided; must be one of 'true'|'false'|'default'
        V_IS_PLANNED_KEY_PROVIDED := (GET_PATH(V_RECORD_DATA, 'is_planned') IS NOT NULL);
        IF (V_IS_PLANNED_KEY_PROVIDED) THEN
            V_IS_PLANNED_STR := LOWER(TRIM(V_RECORD_DATA:is_planned::VARCHAR));
            IF (V_IS_PLANNED_STR NOT IN ('true','false','default')) THEN
                RAISE invalid_is_planned_value_ex;
            END IF;
        ELSE
            V_IS_PLANNED_STR := NULL;
        END IF;
        IF (GET_PATH(V_RECORD_DATA, 'market_code_exclusions') IS NOT NULL AND IS_ARRAY(V_RECORD_DATA:market_code_exclusions)) THEN
            V_MARKET_CODE_EXCLUSIONS := V_RECORD_DATA:market_code_exclusions::ARRAY;
        END IF;
        IF (GET_PATH(V_RECORD_DATA, 'customer_id_exclusions') IS NOT NULL AND IS_ARRAY(V_RECORD_DATA:customer_id_exclusions)) THEN
            V_CUSTOMER_ID_EXCLUSIONS := V_RECORD_DATA:customer_id_exclusions::ARRAY;
        END IF;
        IF (GET_PATH(V_RECORD_DATA, 'tag_names') IS NOT NULL AND IS_ARRAY(V_RECORD_DATA:tag_names)) THEN
            V_TAG_NAMES_ARRAY := V_RECORD_DATA:tag_names::ARRAY;
        ELSE
            V_TAG_NAMES_ARRAY := NULL;
        END IF;
        V_TAG_NAMES_PRESENT_AND_ARRAY := (GET_PATH(V_RECORD_DATA, 'tag_names') IS NOT NULL AND IS_ARRAY(V_RECORD_DATA:tag_names));

        BEGIN
            -- Insert any new tags that don't exist
            IF (V_TAG_NAMES_ARRAY IS NOT NULL AND ARRAY_SIZE(V_TAG_NAMES_ARRAY) > 0) THEN
                INSERT INTO MASTER_DATA.APOLLO_PRODUCT_TAGS (TAG_NAME)
                SELECT DISTINCT INITCAP(input_tags.tag_name)
                FROM (
                    SELECT TRIM(VALUE::VARCHAR) AS tag_name 
                    FROM TABLE(FLATTEN(INPUT => :V_TAG_NAMES_ARRAY))
                    WHERE TRIM(VALUE::VARCHAR) IS NOT NULL 
                      AND TRIM(VALUE::VARCHAR) != ''
                ) input_tags
                WHERE NOT EXISTS (
                    SELECT 1 FROM MASTER_DATA.APOLLO_PRODUCT_TAGS t
                    WHERE INITCAP(TRIM(t.TAG_NAME)) = INITCAP(input_tags.tag_name)
                );

                V_NEW_TAG_COUNT := SQLROWCOUNT;

                WITH normalized_input_tags AS (
                    SELECT DISTINCT INITCAP(TRIM(VALUE::VARCHAR)) AS tag_name_norm 
                    FROM TABLE(FLATTEN(INPUT => :V_TAG_NAMES_ARRAY))
                    WHERE TRIM(VALUE::VARCHAR) IS NOT NULL 
                    AND TRIM(VALUE::VARCHAR) != ''
                ),
                matched_tags AS (
                    SELECT 
                        MIN(apt.TAG_ID) AS tag_id,
                        nit.tag_name_norm
                    FROM normalized_input_tags nit
                    INNER JOIN MASTER_DATA.APOLLO_PRODUCT_TAGS apt
                        ON INITCAP(apt.TAG_NAME) = nit.tag_name_norm
                    GROUP BY nit.tag_name_norm
                )
                SELECT 
                    COALESCE(ARRAY_AGG(tag_id), ARRAY_CONSTRUCT()),
                    COALESCE(ARRAY_AGG(tag_name_norm), ARRAY_CONSTRUCT())
                INTO 
                    :V_PROCESSED_TAG_IDS,
                    :V_PROCESSED_TAG_NAMES
                FROM matched_tags;
            END IF;

            -- Determine if the VSP already exists in the tag table
            SELECT COUNT(*) > 0
            INTO :V_EXISTS_IN_TAG_TABLE
            FROM MASTER_DATA.APOLLO_VARIANT_SIZE_PACK_TAG
            WHERE VARIANT_SIZE_PACK_ID = :V_VARIANT_SIZE_PACK_ID;

            -- Guard: prevent marking a system SKU as custom
            SELECT COUNT(*) > 0 INTO :V_EXISTS_IN_SKU_MASTER
            FROM MASTER_DATA.APOLLO_SKU_MASTER
            WHERE VARIANT_SIZE_PACK_ID = :V_VARIANT_SIZE_PACK_ID;

            IF (COALESCE(:V_IS_CUSTOM_PRODUCT, FALSE) AND V_EXISTS_IN_SKU_MASTER) THEN
                RAISE system_sku_cannot_be_custom_ex;
            END IF;

            IF (V_EXISTS_IN_TAG_TABLE) THEN
                -- Partial update existing row
                UPDATE MASTER_DATA.APOLLO_VARIANT_SIZE_PACK_TAG
                SET 
                    TAG_IDS = CASE WHEN :V_TAG_NAMES_PRESENT_AND_ARRAY THEN :V_PROCESSED_TAG_IDS ELSE TAG_IDS END,
                    TAG_NAMES = CASE WHEN :V_TAG_NAMES_PRESENT_AND_ARRAY THEN :V_PROCESSED_TAG_NAMES ELSE TAG_NAMES END,
                    VARIANT_SIZE_PACK_DESC = CASE WHEN COALESCE(:V_IS_CUSTOM_PRODUCT, IS_CUSTOM_PRODUCT) THEN COALESCE(:V_INPUT_DESC, VARIANT_SIZE_PACK_DESC) ELSE VARIANT_SIZE_PACK_DESC END,
                    IS_PLANNED = COALESCE(:V_IS_PLANNED_STR, IS_PLANNED),
                    MARKET_CODE_EXCLUSIONS = CASE WHEN :V_MARKET_CODE_EXCLUSIONS IS NOT NULL THEN :V_MARKET_CODE_EXCLUSIONS ELSE COALESCE(MARKET_CODE_EXCLUSIONS, ARRAY_CONSTRUCT()) END,
                    CUSTOMER_ID_EXCLUSIONS = CASE WHEN :V_CUSTOMER_ID_EXCLUSIONS IS NOT NULL THEN :V_CUSTOMER_ID_EXCLUSIONS ELSE COALESCE(CUSTOMER_ID_EXCLUSIONS, ARRAY_CONSTRUCT()) END,
                    IS_CUSTOM_PRODUCT = COALESCE(:V_IS_CUSTOM_PRODUCT, IS_CUSTOM_PRODUCT)
                WHERE VARIANT_SIZE_PACK_ID = :V_VARIANT_SIZE_PACK_ID;

                -- Mark for realtime sync if relevant fields were provided
                IF (V_IS_PLANNED_KEY_PROVIDED OR V_MARKET_CODE_EXCLUSIONS IS NOT NULL OR V_CUSTOMER_ID_EXCLUSIONS IS NOT NULL) THEN
                    V_IDS_TO_SYNC := ARRAY_CAT(:V_IDS_TO_SYNC, ARRAY_CONSTRUCT(:V_VARIANT_SIZE_PACK_ID));
                END IF;

                V_SUCCESS_COUNT := V_SUCCESS_COUNT + 1;
            ELSE
                -- Validate SKU master, unless creating a custom product with provided desc
                IF (COALESCE(:V_IS_CUSTOM_PRODUCT, FALSE)) THEN
                    IF (V_INPUT_DESC IS NULL OR TRIM(V_INPUT_DESC) = '') THEN
                        RAISE missing_desc_for_custom_ex;
                    END IF;
                    INSERT INTO MASTER_DATA.APOLLO_VARIANT_SIZE_PACK_TAG
                        (VARIANT_SIZE_PACK_ID, VARIANT_SIZE_PACK_DESC, TAG_IDS, TAG_NAMES, IS_PLANNED, MARKET_CODE_EXCLUSIONS, CUSTOMER_ID_EXCLUSIONS, IS_CUSTOM_PRODUCT)
                    SELECT
                        :V_VARIANT_SIZE_PACK_ID, :V_INPUT_DESC, :V_PROCESSED_TAG_IDS, :V_PROCESSED_TAG_NAMES,
                        :V_IS_PLANNED_STR, COALESCE(:V_MARKET_CODE_EXCLUSIONS, ARRAY_CONSTRUCT()), COALESCE(:V_CUSTOMER_ID_EXCLUSIONS, ARRAY_CONSTRUCT()), TRUE;

                    -- Mark for realtime sync if relevant fields were provided
                    IF (V_IS_PLANNED_KEY_PROVIDED OR V_MARKET_CODE_EXCLUSIONS IS NOT NULL OR V_CUSTOMER_ID_EXCLUSIONS IS NOT NULL) THEN
                        V_IDS_TO_SYNC := ARRAY_CAT(:V_IDS_TO_SYNC, ARRAY_CONSTRUCT(:V_VARIANT_SIZE_PACK_ID));
                    END IF;

                    V_SUCCESS_COUNT := V_SUCCESS_COUNT + 1;
                ELSE
                    SELECT MAX(m.VARIANT_SIZE_PACK_DESC)
                    INTO :V_VSP_DESC_FROM_MASTER
                    FROM MASTER_DATA.APOLLO_SKU_MASTER m 
                    WHERE m.VARIANT_SIZE_PACK_ID = :V_VARIANT_SIZE_PACK_ID;

                    IF (V_VSP_DESC_FROM_MASTER IS NOT NULL) THEN
                        INSERT INTO MASTER_DATA.APOLLO_VARIANT_SIZE_PACK_TAG
                            (VARIANT_SIZE_PACK_ID, VARIANT_SIZE_PACK_DESC, TAG_IDS, TAG_NAMES, IS_PLANNED, MARKET_CODE_EXCLUSIONS, CUSTOMER_ID_EXCLUSIONS, IS_CUSTOM_PRODUCT)
                        SELECT
                            :V_VARIANT_SIZE_PACK_ID, :V_VSP_DESC_FROM_MASTER, :V_PROCESSED_TAG_IDS, :V_PROCESSED_TAG_NAMES,
                            :V_IS_PLANNED_STR, COALESCE(:V_MARKET_CODE_EXCLUSIONS, ARRAY_CONSTRUCT()), COALESCE(:V_CUSTOMER_ID_EXCLUSIONS, ARRAY_CONSTRUCT()), FALSE;

                        -- Mark for realtime sync if relevant fields were provided
                        IF (V_IS_PLANNED_KEY_PROVIDED OR V_MARKET_CODE_EXCLUSIONS IS NOT NULL OR V_CUSTOMER_ID_EXCLUSIONS IS NOT NULL) THEN
                            V_IDS_TO_SYNC := ARRAY_CAT(:V_IDS_TO_SYNC, ARRAY_CONSTRUCT(:V_VARIANT_SIZE_PACK_ID));
                        END IF;

                        V_SUCCESS_COUNT := V_SUCCESS_COUNT + 1;
                    ELSE
                        RAISE vsp_not_found_ex;
                    END IF;
                END IF;
            END IF;

            -- TODO: optional audit insert here with payload and row counts
        EXCEPTION
            WHEN OTHER THEN
                -- Defer rollback until end; make this all-or-nothing
                V_ERROR_COUNT := V_ERROR_COUNT + 1;
        END;
    END LOOP;

    CLOSE json_records_cursor;
    
    -- If any errors occurred, roll back everything and raise
    IF (V_ERROR_COUNT > 0) THEN
        ROLLBACK;
        RAISE processing_failed_ex;
    END IF;

    -- Run realtime sync once for all affected VSPs, as part of the same transaction
    IF (V_IDS_TO_SYNC IS NOT NULL AND ARRAY_SIZE(V_IDS_TO_SYNC) > 0) THEN
        BEGIN
            CALL FORECAST.SP_SYNC_PLANNED_PRODUCTS_REALTIME(:V_IDS_TO_SYNC, 'batch_update');
        EXCEPTION
            WHEN OTHER THEN
                ROLLBACK;
                RAISE processing_failed_ex;
        END;
    END IF;

    COMMIT;
    
    RETURN 'SUCCESS: Batch processing complete. Successful updates: ' || V_SUCCESS_COUNT || '.';
END;
$$;

COMMENT ON PROCEDURE MASTER_DATA.SP_BATCH_UPDATE_APOLLO_VARIANT_SIZE_PACK_TAGS(VARCHAR) IS
'Processes a batch of product tag and planning flag updates from a JSON string. For each item, the procedure
validates/creates tags, then upserts into APOLLO_VARIANT_SIZE_PACK_TAG (including planning/exclusion/custom fields), all inline within the batch.
If any single operation fails, the entire batch is rolled back and a summary exception is raised. A single realtime sync is executed on success.';

-- Example Usage:
-- CALL MASTER_DATA.SP_BATCH_UPDATE_APOLLO_VARIANT_SIZE_PACK_TAGS(
--   '[{"variant_size_pack_id": "HE009-6-750", "tag_names": ["Core", "Luxury"], "is_planned": true}]'
-- ); 
-- ); 