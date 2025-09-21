-- Snowflake Stored Procedure for publishing forecasts - Version 2 Optimized
-- Focus: Simple set-based operations leveraging Snowflake's columnar architecture
-- Eliminates complex CTEs and reduces JOIN operations for maximum performance


CREATE OR REPLACE PROCEDURE FORECAST.SP_PUBLISH_DIVISION_FORECAST(
    P_FORECAST_GENERATION_MONTH_DATE_STR VARCHAR,
    P_USER_ID VARCHAR,
    P_DIVISION VARCHAR,
    P_PUBLICATION_STATUS VARCHAR DEFAULT 'review',
    P_PUBLICATION_NOTE VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    -- Exceptions
    invalid_status_ex EXCEPTION (-20001, 'Invalid publication status. Must be "review" or "consensus".');
    invalid_date_format_ex EXCEPTION (-20002, 'Invalid forecast generation month date format. Must be YYYY-MM-01.');
    missing_division_ex EXCEPTION (-20003, 'Division must be specified when publishing to "review" status.');
    no_divisions_to_promote_ex EXCEPTION (-20004, 'No divisions with review status found to promote to consensus.');
    no_markets_to_promote_ex EXCEPTION (-20005, 'No markets in review status found for division. Markets must be in review status before they can be promoted to consensus.');

    -- Variables
    V_CURRENT_FGMD DATE;
    V_GROUP_ID INTEGER;
    V_MARKETS_PROCESSED_COUNT INTEGER := 0;
    V_MARKETS_SKIPPED_COUNT INTEGER := 0;
    V_DIVISIONS_PROMOTED_COUNT INTEGER := 0;
    V_TOTAL_MARKETS_IN_DIVISION INTEGER;
    V_CONSENSUS_MARKETS_IN_DIVISION INTEGER;

BEGIN
    -- ========= ALL VALIDATION BEFORE ANY TRANSACTIONS =========
    -- Input validation
    IF (P_PUBLICATION_STATUS NOT IN ('review', 'consensus')) THEN
        RAISE invalid_status_ex;
    END IF;
    IF (P_FORECAST_GENERATION_MONTH_DATE_STR NOT RLIKE '^\\d{4}-\\d{2}-01$') THEN
        RAISE invalid_date_format_ex;
    END IF;
    V_CURRENT_FGMD := TO_DATE(P_FORECAST_GENERATION_MONTH_DATE_STR);
    IF (P_PUBLICATION_STATUS = 'review' AND P_DIVISION IS NULL) THEN
        RAISE missing_division_ex;
    END IF;

    -- Business logic validation for consensus promotion
    IF (P_PUBLICATION_STATUS = 'consensus') THEN
        IF (P_DIVISION IS NULL) THEN
            -- Check if there are divisions with review status BEFORE starting transaction
            SELECT COUNT(DISTINCT g.DIVISION) INTO :V_DIVISIONS_PROMOTED_COUNT
            FROM FORECAST.DEPLETIONS_FORECAST_PUBLICATION_GROUPS g 
            JOIN FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS p ON g.GROUP_ID = p.GROUP_ID 
            WHERE g.FORECAST_GENERATION_MONTH_DATE = :V_CURRENT_FGMD AND p.PUBLICATION_STATUS = 'review';
            
            IF (V_DIVISIONS_PROMOTED_COUNT = 0) THEN
                RAISE no_divisions_to_promote_ex;
            END IF;
        ELSE
            -- Check if there are markets in review status for this division BEFORE starting transaction
            SELECT COUNT(DISTINCT p.MARKET_CODE) INTO :V_MARKETS_PROCESSED_COUNT
            FROM FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS p
            JOIN FORECAST.DEPLETIONS_FORECAST_PUBLICATION_GROUPS g ON p.GROUP_ID = g.GROUP_ID
            WHERE g.DIVISION = :P_DIVISION 
              AND g.FORECAST_GENERATION_MONTH_DATE = :V_CURRENT_FGMD
              AND p.PUBLICATION_STATUS = 'review';
            
            IF (V_MARKETS_PROCESSED_COUNT = 0) THEN
                RAISE no_markets_to_promote_ex;
            END IF;
        END IF;
    END IF;

    -- ========= CONSENSUS WORKFLOW (AFTER VALIDATION) =========
    IF (P_PUBLICATION_STATUS = 'consensus') THEN
        BEGIN TRANSACTION;
        
        BEGIN
            -- Single set-based promotion (existing logic is already efficient)
            IF (P_DIVISION IS NULL) THEN
                -- Get list of divisions that are currently in REVIEW status (these will be promoted)
                LET divisions_to_promote_csv VARCHAR := '';
                SELECT LISTAGG(DISTINCT g.DIVISION, ',') INTO :divisions_to_promote_csv
                FROM FORECAST.DEPLETIONS_FORECAST_PUBLICATION_GROUPS g 
                JOIN FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS p ON g.GROUP_ID = p.GROUP_ID 
                WHERE g.FORECAST_GENERATION_MONTH_DATE = :V_CURRENT_FGMD AND p.PUBLICATION_STATUS = 'review';

                -- We already validated there are divisions to promote above
                UPDATE FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS
                SET PUBLICATION_STATUS = 'consensus',
                    APPROVAL_STATUS_DATE = CURRENT_TIMESTAMP(),
                    PUBLISHED_BY_USER_ID = :P_USER_ID,
                    PUBLICATION_NOTE = COALESCE(PUBLICATION_NOTE, '') || ' | Promoted by ' || :P_USER_ID
                WHERE GROUP_ID IN (
                    SELECT DISTINCT g.GROUP_ID 
                    FROM FORECAST.DEPLETIONS_FORECAST_PUBLICATION_GROUPS g 
                    JOIN FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS p ON g.GROUP_ID = p.GROUP_ID 
                    WHERE g.FORECAST_GENERATION_MONTH_DATE = :V_CURRENT_FGMD AND p.PUBLICATION_STATUS = 'review'
                ) AND PUBLICATION_STATUS = 'review';

                UPDATE FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST
                SET FORECAST_STATUS = 'consensus'
                WHERE FORECAST_GENERATION_MONTH_DATE = :V_CURRENT_FGMD
                  AND MARKET_CODE IN (
                    SELECT DISTINCT h.MARKET_CODE 
                    FROM MASTER_DATA.STG_HYPERION__DIVISION_HIERARCHY h
                    WHERE h.DIVISION IN (SELECT TRIM(VALUE) FROM TABLE(SPLIT_TO_TABLE(:divisions_to_promote_csv, ',')))
                  )
                  AND FORECAST_STATUS = 'review';
                
                -- ========= CHAINS PUBLISHING (ALL DIVISIONS) =========
                -- Step 1: Archive manual chains forecasts
                INSERT INTO FORECAST.DEPLETIONS_FORECAST_PUBLISHED_FORECASTS_CHAINS(
                    GROUP_ID, PUBLICATION_ID, SOURCE_TABLE, SOURCE_ID, MARKET_NAME, MARKET_CODE, 
                    DISTRIBUTOR_NAME, DISTRIBUTOR_ID, PARENT_CHAIN_NAME, PARENT_CHAIN_CODE, BRAND, BRAND_ID, 
                    VARIANT, VARIANT_ID, VARIANT_SIZE_PACK_DESC, VARIANT_SIZE_PACK_ID, FORECAST_YEAR, MONTH, 
                    FORECAST_METHOD, FORECAST_GENERATION_MONTH_DATE, CASE_EQUIVALENT_VOLUME, VERSION_NUMBER
                )
                SELECT 
                    p.GROUP_ID, p.PUBLICATION_ID, 'manual', m.ID, m.MARKET_NAME, m.MARKET_CODE, 
                    m.DISTRIBUTOR_NAME, m.DISTRIBUTOR_ID, m.PARENT_CHAIN_NAME, m.PARENT_CHAIN_CODE, 
                    m.BRAND, m.BRAND_ID, m.VARIANT, m.VARIANT_ID, m.VARIANT_SIZE_PACK_DESC, m.VARIANT_SIZE_PACK_ID, 
                    m.FORECAST_YEAR, m.MONTH, 'manual', :V_CURRENT_FGMD, m.MANUAL_CASE_EQUIVALENT_VOLUME, m.CURRENT_VERSION
                FROM FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST_CHAINS m
                JOIN FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS p ON m.MARKET_CODE = p.MARKET_CODE
                WHERE m.MARKET_CODE IN (
                    SELECT DISTINCT h.MARKET_CODE 
                    FROM MASTER_DATA.STG_HYPERION__DIVISION_HIERARCHY h
                    WHERE h.DIVISION IN (SELECT TRIM(VALUE) FROM TABLE(SPLIT_TO_TABLE(:divisions_to_promote_csv, ',')))
                )
                AND p.FORECAST_GENERATION_MONTH_DATE = :V_CURRENT_FGMD
                AND p.PUBLICATION_STATUS = 'consensus'
                AND EXISTS (
                    SELECT 1 FROM FORECAST.DEPLETIONS_FORECAST_INIT_DRAFT_CHAINS d
                    WHERE d.MARKET_CODE = m.MARKET_CODE 
                      AND d.DISTRIBUTOR_ID = m.DISTRIBUTOR_ID 
                      AND d.PARENT_CHAIN_CODE = m.PARENT_CHAIN_CODE 
                      AND d.VARIANT_SIZE_PACK_ID = m.VARIANT_SIZE_PACK_ID 
                      AND d.FORECAST_YEAR = m.FORECAST_YEAR 
                      AND d.MONTH = m.MONTH
                      AND d.FORECAST_GENERATION_MONTH_DATE = :V_CURRENT_FGMD
                      AND d.DATA_TYPE = 'forecast'
                )
                AND (
                    DATE_FROM_PARTS(m.FORECAST_YEAR, m.MONTH, 1) > :V_CURRENT_FGMD
                    OR (
                        DATE_FROM_PARTS(m.FORECAST_YEAR, m.MONTH, 1) = :V_CURRENT_FGMD
                        AND EXISTS (
                            SELECT 1 FROM MASTER_DATA.MARKETS_MASTER mm
                            WHERE mm.MARKET_ID = m.MARKET_CODE AND mm.AREA_DESCRIPTION = 'Control'
                        )
                    )
                );

                -- Removed: FGMD-month projection insert for chains; FGMD now included in growth_forecast using INIT_DRAFT_CHAINS

                -- Step 1.3: Archive actual chains forecasts (months prior to FGMD)
                INSERT INTO FORECAST.DEPLETIONS_FORECAST_PUBLISHED_FORECASTS_CHAINS(
                    GROUP_ID, PUBLICATION_ID, SOURCE_TABLE, MARKET_NAME, MARKET_CODE,
                    DISTRIBUTOR_NAME, DISTRIBUTOR_ID, PARENT_CHAIN_NAME, PARENT_CHAIN_CODE, BRAND, BRAND_ID,
                    VARIANT, VARIANT_ID, VARIANT_SIZE_PACK_DESC, VARIANT_SIZE_PACK_ID, FORECAST_YEAR, MONTH,
                    FORECAST_METHOD, FORECAST_GENERATION_MONTH_DATE, CASE_EQUIVALENT_VOLUME
                )
                SELECT
                    p.GROUP_ID, p.PUBLICATION_ID, 'actual_complete',
                    d.MARKET_NAME, d.MARKET_CODE, d.DISTRIBUTOR_NAME, d.DISTRIBUTOR_ID,
                    d.PARENT_CHAIN_NAME, d.PARENT_CHAIN_CODE,
                    d.BRAND, d.BRAND_ID, d.VARIANT, d.VARIANT_ID, d.VARIANT_SIZE_PACK_DESC, d.VARIANT_SIZE_PACK_ID,
                    d.FORECAST_YEAR, d.MONTH, d.FORECAST_METHOD, :V_CURRENT_FGMD, d.CASE_EQUIVALENT_VOLUME
                FROM FORECAST.DEPLETIONS_FORECAST_INIT_DRAFT_CHAINS d
                JOIN FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS p ON d.MARKET_CODE = p.MARKET_CODE
                JOIN FORECAST.DEPLETIONS_FORECAST_PRIMARY_FORECAST_METHOD_CHAINS pfc 
                    ON d.MARKET_CODE = pfc.MARKET_CODE 
                    AND d.DISTRIBUTOR_ID = pfc.DISTRIBUTOR_ID 
                    AND d.PARENT_CHAIN_CODE = pfc.PARENT_CHAIN_CODE 
                    AND d.VARIANT_SIZE_PACK_ID = pfc.VARIANT_SIZE_PACK_ID 
                    AND d.FORECAST_METHOD = pfc.FORECAST_METHOD
                WHERE d.MARKET_CODE IN (
                    SELECT DISTINCT h.MARKET_CODE 
                    FROM MASTER_DATA.STG_HYPERION__DIVISION_HIERARCHY h
                    WHERE h.DIVISION IN (SELECT TRIM(VALUE) FROM TABLE(SPLIT_TO_TABLE(:divisions_to_promote_csv, ',')))
                )
                AND d.FORECAST_GENERATION_MONTH_DATE = :V_CURRENT_FGMD
                AND p.FORECAST_GENERATION_MONTH_DATE = :V_CURRENT_FGMD
                AND p.PUBLICATION_STATUS = 'consensus'
                AND pfc.IS_PRIMARY_FORECAST_METHOD = 1
                AND d.DATA_TYPE = 'actual_complete'
                AND DATE_FROM_PARTS(d.FORECAST_YEAR, d.MONTH, 1) < :V_CURRENT_FGMD
                ;

                -- Removed: FGMD-month chains fallback; covered by growth_forecast including FGMD with manual-priority for Control


                -- Step 2: Archive draft chains forecasts (where no manual override exists)
                INSERT INTO FORECAST.DEPLETIONS_FORECAST_PUBLISHED_FORECASTS_CHAINS(
                    GROUP_ID, PUBLICATION_ID, SOURCE_TABLE, MARKET_NAME, MARKET_CODE, 
                    DISTRIBUTOR_NAME, DISTRIBUTOR_ID, PARENT_CHAIN_NAME, PARENT_CHAIN_CODE, BRAND, BRAND_ID, 
                    VARIANT, VARIANT_ID, VARIANT_SIZE_PACK_DESC, VARIANT_SIZE_PACK_ID, FORECAST_YEAR, MONTH, 
                    FORECAST_METHOD, FORECAST_GENERATION_MONTH_DATE, CASE_EQUIVALENT_VOLUME
                )
                SELECT 
                    p.GROUP_ID, p.PUBLICATION_ID, 'growth_forecast', d.MARKET_NAME, d.MARKET_CODE, 
                    d.DISTRIBUTOR_NAME, d.DISTRIBUTOR_ID, d.PARENT_CHAIN_NAME, d.PARENT_CHAIN_CODE,
                    d.BRAND, d.BRAND_ID, d.VARIANT, d.VARIANT_ID, d.VARIANT_SIZE_PACK_DESC, d.VARIANT_SIZE_PACK_ID,
                    d.FORECAST_YEAR, d.MONTH, d.FORECAST_METHOD, :V_CURRENT_FGMD, d.CASE_EQUIVALENT_VOLUME
                FROM FORECAST.DEPLETIONS_FORECAST_INIT_DRAFT_CHAINS d
                JOIN FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS p ON d.MARKET_CODE = p.MARKET_CODE
                JOIN FORECAST.DEPLETIONS_FORECAST_PRIMARY_FORECAST_METHOD_CHAINS pfc 
                    ON d.MARKET_CODE = pfc.MARKET_CODE 
                    AND d.DISTRIBUTOR_ID = pfc.DISTRIBUTOR_ID 
                    AND d.PARENT_CHAIN_CODE = pfc.PARENT_CHAIN_CODE 
                    AND d.VARIANT_SIZE_PACK_ID = pfc.VARIANT_SIZE_PACK_ID 
                    AND d.FORECAST_METHOD = pfc.FORECAST_METHOD
                WHERE d.MARKET_CODE IN (
                    SELECT DISTINCT h.MARKET_CODE 
                    FROM MASTER_DATA.STG_HYPERION__DIVISION_HIERARCHY h
                    WHERE h.DIVISION IN (SELECT TRIM(VALUE) FROM TABLE(SPLIT_TO_TABLE(:divisions_to_promote_csv, ',')))
                )
                AND d.FORECAST_GENERATION_MONTH_DATE = :V_CURRENT_FGMD
                AND p.FORECAST_GENERATION_MONTH_DATE = :V_CURRENT_FGMD
                AND p.PUBLICATION_STATUS = 'consensus'
                AND pfc.IS_PRIMARY_FORECAST_METHOD = 1
                AND (
                    (DATE_FROM_PARTS(d.FORECAST_YEAR, d.MONTH, 1) = :V_CURRENT_FGMD AND (
                        NOT EXISTS (
                            SELECT 1 FROM MASTER_DATA.MARKETS_MASTER mm
                            WHERE mm.MARKET_ID = d.MARKET_CODE AND mm.AREA_DESCRIPTION = 'Control'
                        )
                        OR (
                            EXISTS (
                                SELECT 1 FROM MASTER_DATA.MARKETS_MASTER mm
                                WHERE mm.MARKET_ID = d.MARKET_CODE AND mm.AREA_DESCRIPTION = 'Control'
                            )
                            AND NOT EXISTS (
                                SELECT 1 FROM FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST_CHAINS m 
                                WHERE m.MARKET_CODE = d.MARKET_CODE 
                                  AND m.DISTRIBUTOR_ID = d.DISTRIBUTOR_ID 
                                  AND m.PARENT_CHAIN_CODE = d.PARENT_CHAIN_CODE 
                                  AND m.VARIANT_SIZE_PACK_ID = d.VARIANT_SIZE_PACK_ID 
                                  AND m.FORECAST_YEAR = d.FORECAST_YEAR 
                                  AND m.MONTH = d.MONTH
                            )
                        )
                    ))
                    OR
                    (DATE_FROM_PARTS(d.FORECAST_YEAR, d.MONTH, 1) > :V_CURRENT_FGMD AND
                        NOT EXISTS (
                            SELECT 1 FROM FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST_CHAINS m 
                            WHERE m.MARKET_CODE = d.MARKET_CODE 
                              AND m.DISTRIBUTOR_ID = d.DISTRIBUTOR_ID 
                              AND m.PARENT_CHAIN_CODE = d.PARENT_CHAIN_CODE 
                              AND m.VARIANT_SIZE_PACK_ID = d.VARIANT_SIZE_PACK_ID 
                              AND m.FORECAST_YEAR = d.FORECAST_YEAR 
                              AND m.MONTH = d.MONTH
                        )
                    )
                );
                
                -- Sync consensus forecasts to future FGMD for ONLY the divisions that were just promoted
                -- (Version history is now handled within the internal procedure)
                CALL FORECAST._INTERNAL_SP_SYNC_CONSENSUS_TO_NEXT_MONTH(:V_CURRENT_FGMD, :divisions_to_promote_csv, :P_USER_ID);

            ELSE
                -- We already validated there are markets to promote above
                V_DIVISIONS_PROMOTED_COUNT := 1;
                UPDATE FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS
                SET PUBLICATION_STATUS = 'consensus',
                    APPROVAL_STATUS_DATE = CURRENT_TIMESTAMP(),
                    PUBLISHED_BY_USER_ID = :P_USER_ID
                WHERE GROUP_ID IN (
                    SELECT GROUP_ID FROM FORECAST.DEPLETIONS_FORECAST_PUBLICATION_GROUPS
                    WHERE DIVISION = :P_DIVISION AND FORECAST_GENERATION_MONTH_DATE = :V_CURRENT_FGMD
                ) AND PUBLICATION_STATUS = 'review';
                
                UPDATE FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST
                SET FORECAST_STATUS = 'consensus'
                WHERE FORECAST_GENERATION_MONTH_DATE = :V_CURRENT_FGMD
                    AND MARKET_CODE IN (SELECT MARKET_CODE FROM MASTER_DATA.STG_HYPERION__DIVISION_HIERARCHY WHERE DIVISION = :P_DIVISION)
                    AND FORECAST_STATUS = 'review';

                -- ========= CHAINS PUBLISHING (SPECIFIC DIVISION) =========
                -- Step 1: Archive manual chains forecasts
                INSERT INTO FORECAST.DEPLETIONS_FORECAST_PUBLISHED_FORECASTS_CHAINS(
                    GROUP_ID, PUBLICATION_ID, SOURCE_TABLE, SOURCE_ID, MARKET_NAME, MARKET_CODE, 
                    DISTRIBUTOR_NAME, DISTRIBUTOR_ID, PARENT_CHAIN_NAME, PARENT_CHAIN_CODE, BRAND, BRAND_ID, 
                    VARIANT, VARIANT_ID, VARIANT_SIZE_PACK_DESC, VARIANT_SIZE_PACK_ID, FORECAST_YEAR, MONTH, 
                    FORECAST_METHOD, FORECAST_GENERATION_MONTH_DATE, CASE_EQUIVALENT_VOLUME, VERSION_NUMBER
                )
                SELECT 
                    p.GROUP_ID, p.PUBLICATION_ID, 'manual', m.ID, m.MARKET_NAME, m.MARKET_CODE, 
                    m.DISTRIBUTOR_NAME, m.DISTRIBUTOR_ID, m.PARENT_CHAIN_NAME, m.PARENT_CHAIN_CODE, 
                    m.BRAND, m.BRAND_ID, m.VARIANT, m.VARIANT_ID, m.VARIANT_SIZE_PACK_DESC, m.VARIANT_SIZE_PACK_ID, 
                    m.FORECAST_YEAR, m.MONTH, 'manual', :V_CURRENT_FGMD, m.MANUAL_CASE_EQUIVALENT_VOLUME, m.CURRENT_VERSION
                FROM FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST_CHAINS m
                JOIN FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS p ON m.MARKET_CODE = p.MARKET_CODE
                WHERE m.MARKET_CODE IN (
                    SELECT MARKET_CODE FROM MASTER_DATA.STG_HYPERION__DIVISION_HIERARCHY WHERE DIVISION = :P_DIVISION
                )
                AND p.FORECAST_GENERATION_MONTH_DATE = :V_CURRENT_FGMD
                AND p.PUBLICATION_STATUS = 'consensus'
                AND EXISTS (
                    SELECT 1 FROM FORECAST.DEPLETIONS_FORECAST_INIT_DRAFT_CHAINS d
                    WHERE d.MARKET_CODE = m.MARKET_CODE 
                      AND d.DISTRIBUTOR_ID = m.DISTRIBUTOR_ID 
                      AND d.PARENT_CHAIN_CODE = m.PARENT_CHAIN_CODE 
                      AND d.VARIANT_SIZE_PACK_ID = m.VARIANT_SIZE_PACK_ID 
                      AND d.FORECAST_YEAR = m.FORECAST_YEAR 
                      AND d.MONTH = m.MONTH
                      AND d.FORECAST_GENERATION_MONTH_DATE = :V_CURRENT_FGMD
                      AND d.DATA_TYPE = 'forecast'
                )
                AND (
                    DATE_FROM_PARTS(m.FORECAST_YEAR, m.MONTH, 1) > :V_CURRENT_FGMD
                    OR (
                        DATE_FROM_PARTS(m.FORECAST_YEAR, m.MONTH, 1) = :V_CURRENT_FGMD
                        AND EXISTS (
                            SELECT 1 FROM MASTER_DATA.MARKETS_MASTER mm
                            WHERE mm.MARKET_ID = m.MARKET_CODE AND mm.AREA_DESCRIPTION = 'Control'
                        )
                    )
                );

                -- Step 1.3: Archive actual chains forecasts (months prior to FGMD)
                INSERT INTO FORECAST.DEPLETIONS_FORECAST_PUBLISHED_FORECASTS_CHAINS(
                    GROUP_ID, PUBLICATION_ID, SOURCE_TABLE, MARKET_NAME, MARKET_CODE,
                    DISTRIBUTOR_NAME, DISTRIBUTOR_ID, PARENT_CHAIN_NAME, PARENT_CHAIN_CODE, BRAND, BRAND_ID,
                    VARIANT, VARIANT_ID, VARIANT_SIZE_PACK_DESC, VARIANT_SIZE_PACK_ID, FORECAST_YEAR, MONTH,
                    FORECAST_METHOD, FORECAST_GENERATION_MONTH_DATE, CASE_EQUIVALENT_VOLUME
                )
                SELECT
                    p.GROUP_ID, p.PUBLICATION_ID, 'actual_complete',
                    d.MARKET_NAME, d.MARKET_CODE, d.DISTRIBUTOR_NAME, d.DISTRIBUTOR_ID,
                    d.PARENT_CHAIN_NAME, d.PARENT_CHAIN_CODE,
                    d.BRAND, d.BRAND_ID, d.VARIANT, d.VARIANT_ID, d.VARIANT_SIZE_PACK_DESC, d.VARIANT_SIZE_PACK_ID,
                    d.FORECAST_YEAR, d.MONTH, d.FORECAST_METHOD, :V_CURRENT_FGMD, d.CASE_EQUIVALENT_VOLUME
                FROM FORECAST.DEPLETIONS_FORECAST_INIT_DRAFT_CHAINS d
                JOIN FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS p ON d.MARKET_CODE = p.MARKET_CODE
                JOIN FORECAST.DEPLETIONS_FORECAST_PRIMARY_FORECAST_METHOD_CHAINS pfc 
                    ON d.MARKET_CODE = pfc.MARKET_CODE 
                    AND d.DISTRIBUTOR_ID = pfc.DISTRIBUTOR_ID 
                    AND d.PARENT_CHAIN_CODE = pfc.PARENT_CHAIN_CODE 
                    AND d.VARIANT_SIZE_PACK_ID = pfc.VARIANT_SIZE_PACK_ID 
                    AND d.FORECAST_METHOD = pfc.FORECAST_METHOD
                WHERE d.MARKET_CODE IN (
                    SELECT MARKET_CODE FROM MASTER_DATA.STG_HYPERION__DIVISION_HIERARCHY WHERE DIVISION = :P_DIVISION
                )
                AND d.FORECAST_GENERATION_MONTH_DATE = :V_CURRENT_FGMD
                AND p.FORECAST_GENERATION_MONTH_DATE = :V_CURRENT_FGMD
                AND p.PUBLICATION_STATUS = 'consensus'
                AND pfc.IS_PRIMARY_FORECAST_METHOD = 1
                AND d.DATA_TYPE = 'actual_complete'
                AND DATE_FROM_PARTS(d.FORECAST_YEAR, d.MONTH, 1) < :V_CURRENT_FGMD
                ;

                -- Removed: FGMD-month chains fallback; growth_forecast now includes FGMD with manual-priority for Control


                -- Step 2: Archive draft chains forecasts (where no manual override exists)
                INSERT INTO FORECAST.DEPLETIONS_FORECAST_PUBLISHED_FORECASTS_CHAINS(
                    GROUP_ID, PUBLICATION_ID, SOURCE_TABLE, MARKET_NAME, MARKET_CODE, 
                    DISTRIBUTOR_NAME, DISTRIBUTOR_ID, PARENT_CHAIN_NAME, PARENT_CHAIN_CODE, BRAND, BRAND_ID, 
                    VARIANT, VARIANT_ID, VARIANT_SIZE_PACK_DESC, VARIANT_SIZE_PACK_ID, FORECAST_YEAR, MONTH, 
                    FORECAST_METHOD, FORECAST_GENERATION_MONTH_DATE, CASE_EQUIVALENT_VOLUME
                )
                SELECT 
                    p.GROUP_ID, p.PUBLICATION_ID, 'growth_forecast', d.MARKET_NAME, d.MARKET_CODE, 
                    d.DISTRIBUTOR_NAME, d.DISTRIBUTOR_ID, d.PARENT_CHAIN_NAME, d.PARENT_CHAIN_CODE,
                    d.BRAND, d.BRAND_ID, d.VARIANT, d.VARIANT_ID, d.VARIANT_SIZE_PACK_DESC, d.VARIANT_SIZE_PACK_ID,
                    d.FORECAST_YEAR, d.MONTH, d.FORECAST_METHOD, :V_CURRENT_FGMD, d.CASE_EQUIVALENT_VOLUME
                FROM FORECAST.DEPLETIONS_FORECAST_INIT_DRAFT_CHAINS d
                JOIN FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS p ON d.MARKET_CODE = p.MARKET_CODE
                JOIN FORECAST.DEPLETIONS_FORECAST_PRIMARY_FORECAST_METHOD_CHAINS pfc 
                    ON d.MARKET_CODE = pfc.MARKET_CODE 
                    AND d.DISTRIBUTOR_ID = pfc.DISTRIBUTOR_ID 
                    AND d.PARENT_CHAIN_CODE = pfc.PARENT_CHAIN_CODE 
                    AND d.VARIANT_SIZE_PACK_ID = pfc.VARIANT_SIZE_PACK_ID 
                    AND d.FORECAST_METHOD = pfc.FORECAST_METHOD
                WHERE d.MARKET_CODE IN (
                    SELECT MARKET_CODE FROM MASTER_DATA.STG_HYPERION__DIVISION_HIERARCHY WHERE DIVISION = :P_DIVISION
                )
                AND d.FORECAST_GENERATION_MONTH_DATE = :V_CURRENT_FGMD
                AND p.FORECAST_GENERATION_MONTH_DATE = :V_CURRENT_FGMD
                AND p.PUBLICATION_STATUS = 'consensus'
                            AND pfc.IS_PRIMARY_FORECAST_METHOD = 1
            AND (
                (DATE_FROM_PARTS(d.FORECAST_YEAR, d.MONTH, 1) = :V_CURRENT_FGMD AND (
                    NOT EXISTS (
                        SELECT 1 FROM MASTER_DATA.MARKETS_MASTER mm
                        WHERE mm.MARKET_ID = d.MARKET_CODE AND mm.AREA_DESCRIPTION = 'Control'
                    )
                    OR (
                        EXISTS (
                            SELECT 1 FROM MASTER_DATA.MARKETS_MASTER mm
                            WHERE mm.MARKET_ID = d.MARKET_CODE AND mm.AREA_DESCRIPTION = 'Control'
                        )
                        AND NOT EXISTS (
                            SELECT 1 FROM FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST_CHAINS m 
                            WHERE m.MARKET_CODE = d.MARKET_CODE 
                              AND m.DISTRIBUTOR_ID = d.DISTRIBUTOR_ID 
                              AND m.PARENT_CHAIN_CODE = d.PARENT_CHAIN_CODE 
                              AND m.VARIANT_SIZE_PACK_ID = d.VARIANT_SIZE_PACK_ID 
                              AND m.FORECAST_YEAR = d.FORECAST_YEAR 
                              AND m.MONTH = d.MONTH
                        )
                    )
                ))
                OR
                (DATE_FROM_PARTS(d.FORECAST_YEAR, d.MONTH, 1) > :V_CURRENT_FGMD AND
                    NOT EXISTS (
                        SELECT 1 FROM FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST_CHAINS m 
                        WHERE m.MARKET_CODE = d.MARKET_CODE 
                          AND m.DISTRIBUTOR_ID = d.DISTRIBUTOR_ID 
                          AND m.PARENT_CHAIN_CODE = d.PARENT_CHAIN_CODE 
                          AND m.VARIANT_SIZE_PACK_ID = d.VARIANT_SIZE_PACK_ID 
                          AND m.FORECAST_YEAR = d.FORECAST_YEAR 
                          AND m.MONTH = d.MONTH
                    )
                )
            );

                -- Always sync to future FGMD when promoting individual division to consensus
                -- (Version history is now handled within the internal procedure)
                CALL FORECAST._INTERNAL_SP_SYNC_CONSENSUS_TO_NEXT_MONTH(:V_CURRENT_FGMD, :P_DIVISION, :P_USER_ID);
            END IF;
            
            CALL FORECAST._INTERNAL_SP_UPDATE_VALID_FORECAST_MONTH_STATUS('SP_PUBLISH_DIVISION_FORECAST');
            COMMIT;
            RETURN 'SUCCESS: Promoted ' || V_DIVISIONS_PROMOTED_COUNT || ' division(s) to consensus.';
        EXCEPTION
            WHEN OTHER THEN
                ROLLBACK;
                RAISE; -- Re-raise to fail the procedure call
        END;

    -- ========= SIMPLIFIED REVIEW WORKFLOW =========
    ELSE
        BEGIN TRANSACTION;
        
        BEGIN
            -- Step 1: Early exit check - are all markets already in consensus?
            SELECT 
                COUNT(DISTINCT h.MARKET_CODE) as total_markets,
                COUNT(DISTINCT CASE WHEN p.PUBLICATION_STATUS = 'consensus' THEN h.MARKET_CODE END) as consensus_markets
            INTO :V_TOTAL_MARKETS_IN_DIVISION, :V_CONSENSUS_MARKETS_IN_DIVISION
            FROM MASTER_DATA.STG_HYPERION__DIVISION_HIERARCHY h
            LEFT JOIN FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS p 
                ON h.MARKET_CODE = p.MARKET_CODE 
                AND p.FORECAST_GENERATION_MONTH_DATE = :V_CURRENT_FGMD
            WHERE h.DIVISION = :P_DIVISION;
            
            -- Early exit if all markets already in consensus
            IF (V_CONSENSUS_MARKETS_IN_DIVISION = V_TOTAL_MARKETS_IN_DIVISION AND V_TOTAL_MARKETS_IN_DIVISION > 0) THEN
                COMMIT;
                RETURN 'SUCCESS: Processed 0 markets for division ' || P_DIVISION || '. Skipped ' || V_TOTAL_MARKETS_IN_DIVISION || ' markets already in consensus.';
            END IF;
            
            -- Step 2: Get or create publication group
            SELECT g.GROUP_ID INTO :V_GROUP_ID 
            FROM FORECAST.DEPLETIONS_FORECAST_PUBLICATION_GROUPS g 
            WHERE g.DIVISION = :P_DIVISION 
              AND g.FORECAST_GENERATION_MONTH_DATE = :V_CURRENT_FGMD 
              AND g.GROUP_STATUS = 'active' 
            ORDER BY g.PUBLICATION_DATE DESC 
            LIMIT 1;
            
            IF (V_GROUP_ID IS NULL) THEN
                INSERT INTO FORECAST.DEPLETIONS_FORECAST_PUBLICATION_GROUPS(
                    DIVISION, INITIATED_BY_USER_ID, FORECAST_GENERATION_MONTH_DATE, PUBLICATION_NOTE
                )
                VALUES (:P_DIVISION, :P_USER_ID, :V_CURRENT_FGMD, :P_PUBLICATION_NOTE);
                
                SELECT MAX(GROUP_ID) INTO :V_GROUP_ID 
                FROM FORECAST.DEPLETIONS_FORECAST_PUBLICATION_GROUPS 
                WHERE DIVISION = :P_DIVISION 
                  AND FORECAST_GENERATION_MONTH_DATE = :V_CURRENT_FGMD
                  AND GROUP_STATUS = 'active';
            END IF;

            -- Step 3: Update existing review publications (LEGACY BEHAVIOR - only 'review')
            UPDATE FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS 
            SET GROUP_ID = :V_GROUP_ID,
                PUBLISHED_BY_USER_ID = :P_USER_ID,
                PUBLICATION_NOTE = :P_PUBLICATION_NOTE,
                APPROVAL_STATUS_DATE = CURRENT_TIMESTAMP()
            WHERE MARKET_CODE IN (
                SELECT DISTINCT MARKET_CODE 
                FROM MASTER_DATA.STG_HYPERION__DIVISION_HIERARCHY 
                WHERE DIVISION = :P_DIVISION
            )
            AND FORECAST_GENERATION_MONTH_DATE = :V_CURRENT_FGMD
            AND PUBLICATION_STATUS = 'review';

            -- Step 4: Insert new publications for markets without existing publications
            INSERT INTO FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS(
                GROUP_ID, MARKET_CODE, PUBLISHED_BY_USER_ID, FORECAST_GENERATION_MONTH_DATE, 
                PUBLICATION_NOTE, PUBLICATION_STATUS
            )
            SELECT 
                :V_GROUP_ID,
                h.MARKET_CODE,
                :P_USER_ID,
                :V_CURRENT_FGMD,
                :P_PUBLICATION_NOTE,
                'review'
            FROM MASTER_DATA.STG_HYPERION__DIVISION_HIERARCHY h
            WHERE h.DIVISION = :P_DIVISION
            AND NOT EXISTS (
                SELECT 1 FROM FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS p
                WHERE p.MARKET_CODE = h.MARKET_CODE
                AND p.FORECAST_GENERATION_MONTH_DATE = :V_CURRENT_FGMD
                AND p.PUBLICATION_STATUS IN ('review', 'consensus')
            );

            -- Step 5: Clean up existing published forecasts for this division
            DELETE FROM FORECAST.DEPLETIONS_FORECAST_PUBLISHED_FORECASTS 
            WHERE PUBLICATION_ID IN (
                SELECT p.PUBLICATION_ID 
                FROM FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS p
                WHERE p.MARKET_CODE IN (
                    SELECT DISTINCT MARKET_CODE 
                    FROM MASTER_DATA.STG_HYPERION__DIVISION_HIERARCHY 
                    WHERE DIVISION = :P_DIVISION
                )
                AND p.FORECAST_GENERATION_MONTH_DATE = :V_CURRENT_FGMD
                AND p.GROUP_ID = :V_GROUP_ID
            );

            -- Step 6: Archive manual forecasts (simplified - remove problematic LEFT JOIN)
            INSERT INTO FORECAST.DEPLETIONS_FORECAST_PUBLISHED_FORECASTS(
                GROUP_ID, PUBLICATION_ID, SOURCE_TABLE, SOURCE_ID, MARKET_NAME, MARKET_CODE, 
                DISTRIBUTOR_NAME, DISTRIBUTOR_ID, BRAND, BRAND_ID, VARIANT, VARIANT_ID, 
                VARIANT_SIZE_PACK_DESC, VARIANT_SIZE_PACK_ID, FORECAST_YEAR, MONTH, 
                FORECAST_METHOD, FORECAST_GENERATION_MONTH_DATE, CASE_EQUIVALENT_VOLUME, VERSION_NUMBER
            )
            SELECT 
                :V_GROUP_ID,
                p.PUBLICATION_ID,
                'manual',
                m.ID,
                m.MARKET_NAME, m.MARKET_CODE, m.DISTRIBUTOR_NAME, m.DISTRIBUTOR_ID,
                m.BRAND, m.BRAND_ID, m.VARIANT, m.VARIANT_ID, m.VARIANT_SIZE_PACK_DESC, m.VARIANT_SIZE_PACK_ID,
                m.FORECAST_YEAR, m.MONTH, m.FORECAST_METHOD, m.FORECAST_GENERATION_MONTH_DATE,
                m.MANUAL_CASE_EQUIVALENT_VOLUME, m.CURRENT_VERSION
            FROM FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST m
            JOIN FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS p ON m.MARKET_CODE = p.MARKET_CODE
            WHERE m.MARKET_CODE IN (
                SELECT DISTINCT MARKET_CODE 
                FROM MASTER_DATA.STG_HYPERION__DIVISION_HIERARCHY 
                WHERE DIVISION = :P_DIVISION
            )
            AND EXISTS (
                SELECT 1 FROM FORECAST.DEPLETIONS_FORECAST_INIT_DRAFT d
                WHERE d.MARKET_CODE = m.MARKET_CODE 
                    AND d.DISTRIBUTOR_ID = m.DISTRIBUTOR_ID 
                    AND d.VARIANT_SIZE_PACK_ID = m.VARIANT_SIZE_PACK_ID 
                    AND d.FORECAST_YEAR = m.FORECAST_YEAR 
                    AND d.MONTH = m.MONTH
                    AND d.FORECAST_GENERATION_MONTH_DATE = :V_CURRENT_FGMD
                    AND d.DATA_TYPE = 'forecast'
            )
            AND m.FORECAST_GENERATION_MONTH_DATE = :V_CURRENT_FGMD
            AND p.FORECAST_GENERATION_MONTH_DATE = :V_CURRENT_FGMD
            AND (
                DATE_FROM_PARTS(m.FORECAST_YEAR, m.MONTH, 1) > :V_CURRENT_FGMD
                OR (
                    DATE_FROM_PARTS(m.FORECAST_YEAR, m.MONTH, 1) = :V_CURRENT_FGMD
                    AND EXISTS (
                        SELECT 1 FROM MASTER_DATA.MARKETS_MASTER mm
                        WHERE mm.MARKET_ID = m.MARKET_CODE AND mm.AREA_DESCRIPTION = 'Control'
                    )
                )
            )
            AND p.GROUP_ID = :V_GROUP_ID;

            -- Step 6.3: Archive actual forecasts (months prior to FGMD)
            INSERT INTO FORECAST.DEPLETIONS_FORECAST_PUBLISHED_FORECASTS(
                GROUP_ID, PUBLICATION_ID, SOURCE_TABLE, MARKET_NAME, MARKET_CODE, 
                DISTRIBUTOR_NAME, DISTRIBUTOR_ID, BRAND, BRAND_ID, VARIANT, VARIANT_ID, 
                VARIANT_SIZE_PACK_DESC, VARIANT_SIZE_PACK_ID, FORECAST_YEAR, MONTH, 
                FORECAST_METHOD, FORECAST_GENERATION_MONTH_DATE, CASE_EQUIVALENT_VOLUME
            )
            SELECT 
                :V_GROUP_ID,
                p.PUBLICATION_ID,
                'actual_complete',
                d.MARKET_NAME, d.MARKET_CODE, d.DISTRIBUTOR_NAME, d.DISTRIBUTOR_ID,
                d.BRAND, d.BRAND_ID, d.VARIANT, d.VARIANT_ID, d.VARIANT_SIZE_PACK_DESC, d.VARIANT_SIZE_PACK_ID,
                d.FORECAST_YEAR, d.MONTH, d.FORECAST_METHOD, :V_CURRENT_FGMD,
                d.CASE_EQUIVALENT_VOLUME
            FROM FORECAST.DEPLETIONS_FORECAST_INIT_DRAFT d
            JOIN FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS p ON d.MARKET_CODE = p.MARKET_CODE
            JOIN FORECAST.DEPLETIONS_FORECAST_PRIMARY_FORECAST_METHOD pfc 
                ON d.FORECAST_GENERATION_MONTH_DATE = pfc.FORECAST_GENERATION_MONTH_DATE 
                AND d.MARKET_CODE = pfc.MARKET_CODE 
                AND d.DISTRIBUTOR_ID = pfc.DISTRIBUTOR_ID 
                AND d.VARIANT_SIZE_PACK_ID = pfc.VARIANT_SIZE_PACK_ID 
                AND d.FORECAST_METHOD = pfc.FORECAST_METHOD
            WHERE d.MARKET_CODE IN (
                SELECT DISTINCT MARKET_CODE 
                FROM MASTER_DATA.STG_HYPERION__DIVISION_HIERARCHY 
                WHERE DIVISION = :P_DIVISION
            )
            AND d.FORECAST_GENERATION_MONTH_DATE = :V_CURRENT_FGMD
            AND p.FORECAST_GENERATION_MONTH_DATE = :V_CURRENT_FGMD
            AND p.GROUP_ID = :V_GROUP_ID
            AND pfc.IS_PRIMARY_FORECAST_METHOD = 1
            AND d.DATA_TYPE = 'actual_complete'
            AND DATE_FROM_PARTS(d.FORECAST_YEAR, d.MONTH, 1) < :V_CURRENT_FGMD;

            -- Step 7: Archive draft forecasts (simplified)
            INSERT INTO FORECAST.DEPLETIONS_FORECAST_PUBLISHED_FORECASTS(
                GROUP_ID, PUBLICATION_ID, SOURCE_TABLE, MARKET_NAME, MARKET_CODE, 
                DISTRIBUTOR_NAME, DISTRIBUTOR_ID, BRAND, BRAND_ID, VARIANT, VARIANT_ID, 
                VARIANT_SIZE_PACK_DESC, VARIANT_SIZE_PACK_ID, FORECAST_YEAR, MONTH, 
                FORECAST_METHOD, FORECAST_GENERATION_MONTH_DATE, CASE_EQUIVALENT_VOLUME
            )
            SELECT 
                :V_GROUP_ID,
                p.PUBLICATION_ID,
                'growth_forecast',
                d.MARKET_NAME, d.MARKET_CODE, d.DISTRIBUTOR_NAME, d.DISTRIBUTOR_ID,
                d.BRAND, d.BRAND_ID, d.VARIANT, d.VARIANT_ID, d.VARIANT_SIZE_PACK_DESC, d.VARIANT_SIZE_PACK_ID,
                d.FORECAST_YEAR, d.MONTH, d.FORECAST_METHOD, d.FORECAST_GENERATION_MONTH_DATE,
                d.CASE_EQUIVALENT_VOLUME
            FROM FORECAST.DEPLETIONS_FORECAST_INIT_DRAFT d
            JOIN FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS p ON d.MARKET_CODE = p.MARKET_CODE
            JOIN FORECAST.DEPLETIONS_FORECAST_PRIMARY_FORECAST_METHOD pfc 
                ON d.FORECAST_GENERATION_MONTH_DATE = pfc.FORECAST_GENERATION_MONTH_DATE 
                AND d.MARKET_CODE = pfc.MARKET_CODE 
                AND d.DISTRIBUTOR_ID = pfc.DISTRIBUTOR_ID 
                AND d.VARIANT_SIZE_PACK_ID = pfc.VARIANT_SIZE_PACK_ID 
                AND d.FORECAST_METHOD = pfc.FORECAST_METHOD
            WHERE d.MARKET_CODE IN (
                SELECT DISTINCT MARKET_CODE 
                FROM MASTER_DATA.STG_HYPERION__DIVISION_HIERARCHY 
                WHERE DIVISION = :P_DIVISION
            )
            AND d.FORECAST_GENERATION_MONTH_DATE = :V_CURRENT_FGMD
            AND p.FORECAST_GENERATION_MONTH_DATE = :V_CURRENT_FGMD
            AND p.GROUP_ID = :V_GROUP_ID
            AND pfc.IS_PRIMARY_FORECAST_METHOD = 1
            AND (
                (DATE_FROM_PARTS(d.FORECAST_YEAR, d.MONTH, 1) = :V_CURRENT_FGMD AND (
                    NOT EXISTS (
                        SELECT 1 FROM MASTER_DATA.MARKETS_MASTER mm
                        WHERE mm.MARKET_ID = d.MARKET_CODE AND mm.AREA_DESCRIPTION = 'Control'
                    )
                    OR (
                        EXISTS (
                            SELECT 1 FROM MASTER_DATA.MARKETS_MASTER mm
                            WHERE mm.MARKET_ID = d.MARKET_CODE AND mm.AREA_DESCRIPTION = 'Control'
                        )
                        AND NOT EXISTS (
                            SELECT 1 FROM FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST m
                            WHERE m.MARKET_CODE = d.MARKET_CODE
                              AND m.DISTRIBUTOR_ID = d.DISTRIBUTOR_ID
                              AND m.VARIANT_SIZE_PACK_ID = d.VARIANT_SIZE_PACK_ID
                              AND m.FORECAST_YEAR = d.FORECAST_YEAR
                              AND m.MONTH = d.MONTH
                              AND m.FORECAST_GENERATION_MONTH_DATE = d.FORECAST_GENERATION_MONTH_DATE
                        )
                    )
                ))
                OR
                (DATE_FROM_PARTS(d.FORECAST_YEAR, d.MONTH, 1) > :V_CURRENT_FGMD AND
                    NOT EXISTS (
                        SELECT 1 FROM FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST m
                        WHERE m.MARKET_CODE = d.MARKET_CODE
                          AND m.DISTRIBUTOR_ID = d.DISTRIBUTOR_ID
                          AND m.VARIANT_SIZE_PACK_ID = d.VARIANT_SIZE_PACK_ID
                          AND m.FORECAST_YEAR = d.FORECAST_YEAR
                          AND m.MONTH = d.MONTH
                          AND m.FORECAST_GENERATION_MONTH_DATE = d.FORECAST_GENERATION_MONTH_DATE
                    )
                )
            );

            -- Step 8: Update manual input statuses
            UPDATE FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST 
            SET FORECAST_STATUS = 'review' 
            WHERE MARKET_CODE IN (
                SELECT DISTINCT MARKET_CODE 
                FROM MASTER_DATA.STG_HYPERION__DIVISION_HIERARCHY 
                WHERE DIVISION = :P_DIVISION
            ) 
            AND FORECAST_GENERATION_MONTH_DATE = :V_CURRENT_FGMD;

            -- Step 9: Get final counts
            SELECT 
                COUNT(CASE WHEN p.PUBLICATION_STATUS = 'review' THEN 1 END),
                COUNT(CASE WHEN p.PUBLICATION_STATUS = 'consensus' THEN 1 END)
            INTO :V_MARKETS_PROCESSED_COUNT, :V_MARKETS_SKIPPED_COUNT
            FROM FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS p
            WHERE p.MARKET_CODE IN (
                SELECT DISTINCT MARKET_CODE 
                FROM MASTER_DATA.STG_HYPERION__DIVISION_HIERARCHY 
                WHERE DIVISION = :P_DIVISION
            )
            AND p.FORECAST_GENERATION_MONTH_DATE = :V_CURRENT_FGMD;
            
            COMMIT;
            RETURN 'SUCCESS: Processed ' || V_MARKETS_PROCESSED_COUNT || ' markets for division ' || P_DIVISION || '. Skipped ' || V_MARKETS_SKIPPED_COUNT || ' markets already in consensus.';
        EXCEPTION
            WHEN OTHER THEN
                ROLLBACK;
                RAISE; -- Re-raise to fail the procedure call
        END;
    END IF;

-- Remove EXCEPTION block entirely - let all exceptions bubble up unhandled
-- This makes SQL version fully consistent with Python/JavaScript versions
END;
$$;

COMMENT ON PROCEDURE FORECAST.SP_PUBLISH_DIVISION_FORECAST(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR) IS 
'V3 WITH CHAINS: Simple set-based operations avoiding complex CTEs and excessive JOINs. 
Includes early exit optimization for consensus-only divisions.
CHAINS INTEGRATION: Automatically publishes manual and draft chains forecasts during consensus promotion.'; 