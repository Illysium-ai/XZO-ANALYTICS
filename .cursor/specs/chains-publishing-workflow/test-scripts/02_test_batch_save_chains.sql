-- Chains Publishing Workflow - Test Batch Save Functionality
-- Tests the sp_batch_save_forecasts_chains procedure with running table behavior

USE DATABASE APOLLO_DEVELOPMENT;

-- =========================================
-- TEST 1: BASIC BATCH SAVE (NEW RECORDS)
-- =========================================

SELECT '🧪 TEST 1: Basic Batch Save (New Records)' AS TEST_NAME;

-- Test JSON payload for batch save using real data
-- Count records before batch save
SELECT COUNT(*) AS RECORDS_BEFORE_BATCH_1
FROM FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST_CHAINS 
WHERE MARKET_CODE IN ('USANY1', 'USAPA1')
AND PARENT_CHAIN_CODE IN ('24027', '50018');

-- Execute batch save
CALL FORECAST.SP_BATCH_SAVE_FORECASTS_CHAINS(
    '[{"market_code": "USAFL1", "parent_chain_code": "23922", "variant_size_pack_id": "TD001-6-1000", "forecast_method": "twelve_month", "comment": "DK chains test for USAFL1 primary method only"},
    {"market_code": "USANY1", "parent_chain_code": "24027", "variant_size_pack_id": "TD001-12-750", "forecast_method": "twelve_month", "comment": "DK chains test for USANY1 primary method only"}]',
    'dk_test_user',
    'manual'
);

CALL FORECAST.SP_BATCH_SAVE_FORECASTS_CHAINS(
    '[{"market_code": "USAFL1", "parent_chain_code": "23922", "variant_size_pack_id": "TD001-6-1000", "forecast_year": 2025, "month": 12, "manual_case_equivalent_volume": 325, "comment": "DK chains test for USAFL1"}]',
    'dk_test_user',
    'manual'
);

CALL FORECAST.SP_BATCH_SAVE_FORECASTS_CHAINS(
    '[{"market_code": "USANY1", "parent_chain_code": "24027", "variant_size_pack_id": "TD001-12-750", "forecast_year": 2025, "month": 10, "manual_case_equivalent_volume": 300, "comment": "DK chains test for USANY1"},
    {"market_code": "USAPA1", "parent_chain_code": "50018", "variant_size_pack_id": "SJ001-12-750", "forecast_year": 2025, "month": 11, "manual_case_equivalent_volume": 325, "comment": "DK chains test for USAPA1"},
    {"market_code": "USAFL1", "parent_chain_code": "23922", "variant_size_pack_id": "TD001-6-1000", "forecast_year": 2025, "month": 12, "manual_case_equivalent_volume": 325, "comment": "DK chains test for USAFL1"}]',
    'dk_test_user',
    'manual'
);

-- Count records after batch save
SELECT COUNT(*) AS RECORDS_AFTER_BATCH_1
FROM FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST_CHAINS 
WHERE MARKET_CODE IN ('USANY1', 'USAPA1')
AND PARENT_CHAIN_CODE IN ('24027', '50018');

-- Verify data was saved correctly (running table behavior - no FGMD filtering)
SELECT 
    MARKET_CODE, PARENT_CHAIN_CODE, VARIANT_SIZE_PACK_ID, FORECAST_YEAR, MONTH,
    MANUAL_CASE_EQUIVALENT_VOLUME, UPDATED_BY_USER_ID, FORECAST_STATUS, COMMENT
FROM FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST_CHAINS 
WHERE MARKET_CODE IN ('USANY1', 'USAPA1')
AND PARENT_CHAIN_CODE IN ('24027', '50018')
ORDER BY MARKET_CODE, PARENT_CHAIN_CODE;

-- =========================================
-- TEST 2: BATCH SAVE UPDATE (EXISTING RECORDS)
-- =========================================

SELECT '🧪 TEST 2: Batch Save Update (Existing Records)' AS TEST_NAME;

-- Update existing records with new values using real data
SET TEST_JSON_2 = '[
    {
        "market_code": "USANY1",
        "customer_id": "REAL_DIST_001",
        "parent_chain_code": "24027",
        "variant_size_pack_id": "TD001-12-750",
        "forecast_year": 2025,
        "month": 11,
        "manual_case_equivalent_volume": 350.0,
        "brand": "Real Brand",
        "brand_id": "REAL_BRD_001",
        "variant": "Real Variant",
        "variant_id": "REAL_VAR_001",
        "variant_size_pack_desc": "Real Pack 750ml",
        "comment": "UPDATED chains forecast via batch save for USANY1",
        "forecast_method": "manual"
    }
]';

-- Get version before update
SELECT CURRENT_VERSION AS VERSION_BEFORE_UPDATE
FROM FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST_CHAINS 
WHERE MARKET_CODE IN ('USANY1', 'USAPA1') AND PARENT_CHAIN_CODE = '26097'
AND VARIANT_SIZE_PACK_ID = 'TD001-12-750' AND FORECAST_YEAR = 2025 AND MONTH = 11;

-- Execute batch save update
CALL FORECAST.SP_BATCH_SAVE_FORECASTS_CHAINS($TEST_JSON_2, 'test_user_update', 'draft');

-- Verify update happened and version incremented
SELECT 
    MARKET_CODE, PARENT_CHAIN_CODE, MANUAL_CASE_EQUIVALENT_VOLUME, 
    CURRENT_VERSION, UPDATED_BY_USER_ID, COMMENT
FROM FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST_CHAINS 
WHERE MARKET_CODE IN ('USANY1', 'USAPA1') AND PARENT_CHAIN_CODE = '26097'
AND VARIANT_SIZE_PACK_ID = 'TD001-12-750' AND FORECAST_YEAR = 2025 AND MONTH = 11;

-- Verify version history was created
SELECT VERSION_NUMBER, MANUAL_CASE_EQUIVALENT_VOLUME, UPDATED_BY_USER_ID, COMMENT
FROM FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST_CHAINS_VERSIONS v
JOIN FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST_CHAINS m ON v.FORECAST_ID = m.ID
WHERE m.MARKET_CODE = 'USANY1' AND m.PARENT_CHAIN_CODE = '26097'
AND m.VARIANT_SIZE_PACK_ID = 'TD001-12-750' AND m.FORECAST_YEAR = 2025 AND m.MONTH = 11
ORDER BY VERSION_NUMBER;

-- =========================================
-- TEST 3: VALIDATION ERRORS
-- =========================================

SELECT '🧪 TEST 3: Validation Errors' AS TEST_NAME;

-- Test missing required fields
SET TEST_JSON_INVALID = '[
    {
        "parent_chain_code": "99999",
        "variant_size_pack_id": "INVALID_VSP",
        "forecast_year": 2025,
        "month": 10,
        "manual_case_equivalent_volume": 500.0
    }
]';

-- This should fail due to missing market_code
BEGIN
    CALL FORECAST.SP_BATCH_SAVE_FORECASTS_CHAINS($TEST_JSON_INVALID, 'test_user', 'draft');
    SELECT 'ERROR: Should have failed validation' AS RESULT;
EXCEPTION
    WHEN OTHER THEN
        SELECT 'SUCCESS: Validation error caught as expected - ' || SQLERRM AS RESULT;
END;

-- =========================================
-- TEST 4: PRIMARY METHOD UPDATE
-- =========================================

SELECT '🧪 TEST 4: Primary Method Update' AS TEST_NAME;

-- Insert primary method record for testing using real data
INSERT INTO FORECAST.DEPLETIONS_FORECAST_PRIMARY_FORECAST_METHOD_CHAINS (
    MARKET_NAME, MARKET_CODE, DISTRIBUTOR_ID, PARENT_CHAIN_CODE,
    VARIANT_SIZE_PACK_DESC, VARIANT_SIZE_PACK_ID, FORECAST_METHOD, IS_PRIMARY_FORECAST_METHOD
)
SELECT 
    'New York', 'USANY1', 'REAL_DIST_001', '26097',
    'Real Pack 750ml', 'TD001-12-750', 'three_month', 1
WHERE NOT EXISTS (
    SELECT 1 FROM FORECAST.DEPLETIONS_FORECAST_PRIMARY_FORECAST_METHOD_CHAINS
    WHERE MARKET_CODE IN ('USANY1', 'USAPA1') AND PARENT_CHAIN_CODE = '26097' 
    AND VARIANT_SIZE_PACK_ID = 'TD001-12-750' AND DISTRIBUTOR_ID = 'REAL_DIST_001'
);

-- Check current primary method
SELECT FORECAST_METHOD AS CURRENT_PRIMARY_METHOD
FROM FORECAST.DEPLETIONS_FORECAST_PRIMARY_FORECAST_METHOD_CHAINS
WHERE MARKET_CODE IN ('USANY1', 'USAPA1') AND PARENT_CHAIN_CODE = '26097' 
AND VARIANT_SIZE_PACK_ID = 'TD001-12-750' AND DISTRIBUTOR_ID = 'REAL_DIST_001';

-- Update primary method via batch save
SET TEST_JSON_METHOD = '[
    {
        "market_code": "USANY1",
        "customer_id": "REAL_DIST_001",
        "parent_chain_code": "26097",
        "variant_size_pack_id": "TD001-12-750",
        "forecast_method": "six_month"
    }
]';

CALL FORECAST.SP_BATCH_SAVE_FORECASTS_CHAINS($TEST_JSON_METHOD, 'test_user', 'draft');

-- Verify primary method was updated
SELECT FORECAST_METHOD AS UPDATED_PRIMARY_METHOD
FROM FORECAST.DEPLETIONS_FORECAST_PRIMARY_FORECAST_METHOD_CHAINS
WHERE MARKET_CODE IN ('USANY1', 'USAPA1') AND PARENT_CHAIN_CODE = '26097' 
AND VARIANT_SIZE_PACK_ID = 'TD001-12-750' AND DISTRIBUTOR_ID = 'REAL_DIST_001';

-- =========================================
-- TEST SUMMARY
-- =========================================

SELECT '📊 BATCH SAVE TEST SUMMARY' AS SECTION;

-- Final record counts
SELECT 
    'CHAINS_MANUAL_RECORDS' AS METRIC,
    COUNT(*) AS COUNT
FROM FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST_CHAINS 
WHERE MARKET_CODE IN ('TEST_MKT_001', 'TEST_MKT_002')

UNION ALL

SELECT 
    'CHAINS_VERSION_RECORDS' AS METRIC,
    COUNT(*) AS COUNT
FROM FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST_CHAINS_VERSIONS v
JOIN FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST_CHAINS m ON v.FORECAST_ID = m.ID
WHERE m.MARKET_CODE IN ('TEST_MKT_001', 'TEST_MKT_002')

UNION ALL

SELECT 
    'CHAINS_PRIMARY_METHODS' AS METRIC,
    COUNT(*) AS COUNT
FROM FORECAST.DEPLETIONS_FORECAST_PRIMARY_FORECAST_METHOD_CHAINS
WHERE MARKET_CODE IN ('TEST_MKT_001', 'TEST_MKT_002');

COMMIT;

SELECT '✅ BATCH SAVE TESTS COMPLETE' AS STATUS;