-- Chains Publishing Workflow - Complete Test Suite
-- Executes all test scripts in sequence with proper error handling

USE DATABASE APOLLO_DEVELOPMENT;

-- =========================================
-- TEST SUITE EXECUTION
-- =========================================

SELECT '🧪 STARTING CHAINS PUBLISHING WORKFLOW TEST SUITE' AS STATUS;
SELECT 'Test Environment: ' || CURRENT_DATABASE() || '.' || CURRENT_SCHEMA() AS INFO;
SELECT 'Current Time: ' || CURRENT_TIMESTAMP() AS TIMESTAMP;
SELECT 'Current FGMD: ' || FORECAST.UDF_GET_VALID_FORECAST_GENERATION_MONTH_DATE() AS FGMD;

-- =========================================
-- PRE-TEST VERIFICATION
-- =========================================

-- Verify required procedures exist
SELECT 'Verifying stored procedures exist...' AS STEP;

SELECT 
    PROCEDURE_NAME, 
    CASE WHEN PROCEDURE_NAME IS NOT NULL THEN '✅ EXISTS' ELSE '❌ MISSING' END AS STATUS
FROM (
    SELECT 'SP_BATCH_SAVE_FORECASTS_CHAINS' AS EXPECTED_PROC
    UNION ALL SELECT 'SP_PUBLISH_DIVISION_FORECAST'
    UNION ALL SELECT 'SP_UNPUBLISH_DIVISION_FORECAST'  
    UNION ALL SELECT 'SP_UNPUBLISH_MARKET_FORECAST'
) expected
LEFT JOIN INFORMATION_SCHEMA.PROCEDURES p 
    ON p.PROCEDURE_NAME = expected.EXPECTED_PROC 
    AND p.PROCEDURE_SCHEMA = 'FORECAST'
ORDER BY expected.EXPECTED_PROC;

-- Verify required tables exist
SELECT 'Verifying tables exist...' AS STEP;

SELECT 
    TABLE_NAME,
    CASE WHEN TABLE_NAME IS NOT NULL THEN '✅ EXISTS' ELSE '❌ MISSING' END AS STATUS
FROM (
    SELECT 'MANUAL_INPUT_DEPLETIONS_FORECAST_CHAINS' AS EXPECTED_TABLE
    UNION ALL SELECT 'DEPLETIONS_FORECAST_PUBLISHED_FORECASTS_CHAINS'
    UNION ALL SELECT 'DEPLETIONS_FORECAST_INIT_DRAFT_CHAINS'
    UNION ALL SELECT 'DEPLETIONS_FORECAST_PRIMARY_FORECAST_METHOD_CHAINS'
) expected
LEFT JOIN INFORMATION_SCHEMA.TABLES t 
    ON t.TABLE_NAME = expected.EXPECTED_TABLE 
    AND t.TABLE_SCHEMA = 'FORECAST'
ORDER BY expected.EXPECTED_TABLE;

-- =========================================
-- EXECUTE TEST SEQUENCE
-- =========================================

BEGIN
    SELECT '📋 TEST 1: DATA SETUP' AS CURRENT_TEST;
    !source 01_test_data_setup.sql;
    SELECT '✅ TEST 1 COMPLETED' AS RESULT;
EXCEPTION
    WHEN OTHER THEN
        SELECT '❌ TEST 1 FAILED: ' || SQLERRM AS RESULT;
        RAISE;
END;

BEGIN
    SELECT '📋 TEST 2: BATCH SAVE' AS CURRENT_TEST;
    !source 02_test_batch_save_chains.sql;
    SELECT '✅ TEST 2 COMPLETED' AS RESULT;
EXCEPTION
    WHEN OTHER THEN
        SELECT '❌ TEST 2 FAILED: ' || SQLERRM AS RESULT;
        RAISE;
END;

BEGIN
    SELECT '📋 TEST 3: PUBLISHING' AS CURRENT_TEST;
    !source 03_test_publishing_chains.sql;
    SELECT '✅ TEST 3 COMPLETED' AS RESULT;
EXCEPTION
    WHEN OTHER THEN
        SELECT '❌ TEST 3 FAILED: ' || SQLERRM AS RESULT;
        RAISE;
END;

BEGIN
    SELECT '📋 TEST 4: DIVISION UNPUBLISH' AS CURRENT_TEST;
    !source 04_test_unpublish_division.sql;
    SELECT '✅ TEST 4 COMPLETED' AS RESULT;
EXCEPTION
    WHEN OTHER THEN
        SELECT '❌ TEST 4 FAILED: ' || SQLERRM AS RESULT;
        RAISE;
END;

BEGIN
    SELECT '📋 TEST 5: MARKET UNPUBLISH' AS CURRENT_TEST;
    !source 05_test_unpublish_market.sql;
    SELECT '✅ TEST 5 COMPLETED' AS RESULT;
EXCEPTION
    WHEN OTHER THEN
        SELECT '❌ TEST 5 FAILED: ' || SQLERRM AS RESULT;
        RAISE;
END;

BEGIN
    SELECT '📋 TEST 6: CLEANUP' AS CURRENT_TEST;
    !source 06_test_cleanup.sql;
    SELECT '✅ TEST 6 COMPLETED' AS RESULT;
EXCEPTION
    WHEN OTHER THEN
        SELECT '❌ TEST 6 FAILED: ' || SQLERRM AS RESULT;
        -- Continue with cleanup even if some steps fail
END;

-- =========================================
-- FINAL TEST SUMMARY
-- =========================================

SELECT '🎯 CHAINS PUBLISHING WORKFLOW TEST SUITE SUMMARY' AS SECTION;

-- Performance timing would be added here if needed
SELECT 'All tests completed at: ' || CURRENT_TIMESTAMP() AS COMPLETION_TIME;

-- Verify clean state
SELECT 
    CASE 
        WHEN (
            SELECT COUNT(*) FROM FORECAST.DEPLETIONS_FORECAST_PUBLISHED_FORECASTS_CHAINS 
            WHERE MARKET_CODE IN ('USANY1', 'USAPA1')
        ) = 0 
        AND (
            SELECT COUNT(*) FROM FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS 
            WHERE MARKET_CODE IN ('USANY1', 'USAPA1')
        ) = 0
        THEN '✅ ENVIRONMENT CLEAN - ALL TEST DATA REMOVED'
        ELSE '⚠️  ENVIRONMENT NOT FULLY CLEAN - MANUAL CLEANUP MAY BE NEEDED'
    END AS FINAL_STATE;

-- Test Results Summary
SELECT '📊 TEST COVERAGE COMPLETED:' AS COVERAGE;
SELECT '✅ Chains Batch Save with Running Table Behavior' AS TEST_COVERED;
SELECT '✅ Chains Publishing during Consensus Promotion' AS TEST_COVERED;
SELECT '✅ Chains NOT Publishing during Review' AS TEST_COVERED;
SELECT '✅ Shared Publication Infrastructure (group_id/publication_id)' AS TEST_COVERED;
SELECT '✅ Manual + Draft Chains Publishing' AS TEST_COVERED;
SELECT '✅ Division-Level Unpublishing with Chains' AS TEST_COVERED;
SELECT '✅ Market-Level Unpublishing with Chains' AS TEST_COVERED;
SELECT '✅ Transactional Consistency (Core + Chains together)' AS TEST_COVERED;
SELECT '✅ No Orphaned Records Validation' AS TEST_COVERED;
SELECT '✅ Publication Status Management' AS TEST_COVERED;

COMMIT;

SELECT '🎉 CHAINS PUBLISHING WORKFLOW TEST SUITE COMPLETE!' AS FINAL_STATUS;