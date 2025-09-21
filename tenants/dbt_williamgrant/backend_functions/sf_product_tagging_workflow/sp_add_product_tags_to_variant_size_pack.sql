-- Snowflake Stored Procedure for add_product_tags_to_variant_size_pack
-- Target Schema: APOLLO_WILLIAMGRANT.MASTER_DATA


CREATE OR REPLACE PROCEDURE MASTER_DATA.SP_ADD_PRODUCT_TAGS_TO_VARIANT_SIZE_PACK(
    P_VARIANT_SIZE_PACK_ID VARCHAR,
    P_TAG_NAMES_INPUT ARRAY -- Array of VARCHAR tag names
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    -- Exceptions
    vsp_not_found_ex EXCEPTION (-20051, 'Variant Size Pack ID not found in master data.');

    -- Variables
    V_PROCESSED_TAG_IDS ARRAY := ARRAY_CONSTRUCT();
    V_PROCESSED_TAG_NAMES ARRAY := ARRAY_CONSTRUCT();
    V_EXISTS_IN_TAG_TABLE BOOLEAN;
    V_VSP_DESC_FROM_MASTER VARCHAR;
    V_NEW_TAG_COUNT INTEGER;
BEGIN
    BEGIN TRANSACTION;

    BEGIN
        -- Step 1: Process the input array of tag names using set-based operations
        -- Create any tags that don't exist and build clean arrays of IDs and names
        IF (P_TAG_NAMES_INPUT IS NOT NULL AND ARRAY_SIZE(P_TAG_NAMES_INPUT) > 0) THEN
            
            -- Insert any new tags that don't exist
            INSERT INTO MASTER_DATA.APOLLO_PRODUCT_TAGS (TAG_NAME)
            SELECT DISTINCT input_tags.tag_name
            FROM (
                SELECT TRIM(VALUE::VARCHAR) AS tag_name 
                FROM TABLE(FLATTEN(INPUT => :P_TAG_NAMES_INPUT))
                WHERE TRIM(VALUE::VARCHAR) IS NOT NULL 
                AND TRIM(VALUE::VARCHAR) != ''
            ) input_tags
            LEFT JOIN MASTER_DATA.APOLLO_PRODUCT_TAGS existing_tags
                ON existing_tags.TAG_NAME = input_tags.tag_name
            WHERE existing_tags.TAG_NAME IS NULL;

            -- Get the count of new tags inserted
            V_NEW_TAG_COUNT := SQLROWCOUNT;

            -- Build arrays of processed tag IDs and names using set-based operations
            WITH processed_tags AS (
                SELECT 
                    apt.TAG_ID,
                    apt.TAG_NAME
                FROM (
                    SELECT DISTINCT TRIM(VALUE::VARCHAR) AS tag_name 
                    FROM TABLE(FLATTEN(INPUT => :P_TAG_NAMES_INPUT))
                    WHERE TRIM(VALUE::VARCHAR) IS NOT NULL 
                    AND TRIM(VALUE::VARCHAR) != ''
                ) input_tags
                INNER JOIN MASTER_DATA.APOLLO_PRODUCT_TAGS apt
                    ON apt.TAG_NAME = input_tags.tag_name
                ORDER BY apt.TAG_NAME
            )
            SELECT 
                COALESCE(ARRAY_AGG(TAG_ID), ARRAY_CONSTRUCT()) AS tag_ids,
                COALESCE(ARRAY_AGG(TAG_NAME), ARRAY_CONSTRUCT()) AS tag_names
            INTO 
                :V_PROCESSED_TAG_IDS,
                :V_PROCESSED_TAG_NAMES
            FROM processed_tags;

        END IF;

        -- Step 2: Determine if the VSP already has a record in the tag table
        SELECT COUNT(*) > 0
        INTO :V_EXISTS_IN_TAG_TABLE
        FROM MASTER_DATA.APOLLO_VARIANT_SIZE_PACK_TAG
        WHERE VARIANT_SIZE_PACK_ID = :P_VARIANT_SIZE_PACK_ID;
        
        -- Step 3: Perform the INSERT or UPDATE operation
        IF (V_EXISTS_IN_TAG_TABLE) THEN
            -- UPDATE existing record
            UPDATE MASTER_DATA.APOLLO_VARIANT_SIZE_PACK_TAG
            SET TAG_IDS = :V_PROCESSED_TAG_IDS, TAG_NAMES = :V_PROCESSED_TAG_NAMES
            WHERE VARIANT_SIZE_PACK_ID = :P_VARIANT_SIZE_PACK_ID;
            COMMIT;
            RETURN 'SUCCESS: Tags updated for ' || P_VARIANT_SIZE_PACK_ID;
        ELSE
            -- INSERT new record, but first validate the VSP exists in the master SKU table
            SELECT MAX(m.VARIANT_SIZE_PACK_DESC)
            INTO :V_VSP_DESC_FROM_MASTER
            FROM MASTER_DATA.APOLLO_SKU_MASTER m 
            WHERE m.VARIANT_SIZE_PACK_ID = :P_VARIANT_SIZE_PACK_ID;
            
            IF (V_VSP_DESC_FROM_MASTER IS NOT NULL) THEN
                INSERT INTO MASTER_DATA.APOLLO_VARIANT_SIZE_PACK_TAG
                    (VARIANT_SIZE_PACK_ID, VARIANT_SIZE_PACK_DESC, TAG_IDS, TAG_NAMES)
                VALUES
                    (:P_VARIANT_SIZE_PACK_ID, :V_VSP_DESC_FROM_MASTER, :V_PROCESSED_TAG_IDS, :V_PROCESSED_TAG_NAMES);
                COMMIT;
                RETURN 'SUCCESS: New Variant Size Pack added with tags for ' || P_VARIANT_SIZE_PACK_ID;
            ELSE
                RAISE vsp_not_found_ex;
            END IF;
        END IF;
    EXCEPTION
        WHEN OTHER THEN
            ROLLBACK;
            RAISE; -- Re-raise to fail the procedure call
    END;
END;
$$;

COMMENT ON PROCEDURE MASTER_DATA.SP_ADD_PRODUCT_TAGS_TO_VARIANT_SIZE_PACK(VARCHAR, ARRAY) IS
'Adds/updates product tags for a given variant_size_pack_id. 
If a tag name does not exist in APOLLO_PRODUCT_TAGS, it is created. 
Tag IDs and names are stored as arrays in APOLLO_VARIANT_SIZE_PACK_TAG.';

-- Example Usage:
-- CALL MASTER_DATA.SP_ADD_PRODUCT_TAGS_TO_VARIANT_SIZE_PACK('HE009-6-750', ARRAY_CONSTRUCT('New Tag', 'Existing Tag'));
-- CALL MASTER_DATA.SP_ADD_PRODUCT_TAGS_TO_VARIANT_SIZE_PACK('NONEXISTENTVSP001', ARRAY_CONSTRUCT('Test Tag')); 