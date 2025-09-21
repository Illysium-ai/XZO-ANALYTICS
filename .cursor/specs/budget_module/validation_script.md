# Budget Module Validation Script

This script provides a comprehensive test suite to validate that the Budget Module is working correctly in your environment. Run these queries in sequence to verify all functionality.

## Prerequisites

1. Ensure you have a published forecast baseline with consensus status
2. Have appropriate permissions on the FORECAST schema
3. Replace placeholders with actual values from your environment

## Step 1: Environment Validation

```sql
-- Check database and schema access
USE DATABASE APOLLO_DEVELOPMENT;
USE SCHEMA FORECAST;
SELECT CURRENT_DATABASE(), CURRENT_SCHEMA();

-- Verify tables exist
SELECT 
    TABLE_NAME,
    TABLE_TYPE,
    ROW_COUNT,
    CREATED
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'FORECAST' 
  AND TABLE_NAME LIKE '%BUDGET%'
ORDER BY TABLE_NAME;

-- Verify procedures exist
SELECT 
    PROCEDURE_NAME,
    CREATED,
    PROCEDURE_DEFINITION
FROM INFORMATION_SCHEMA.PROCEDURES
WHERE PROCEDURE_SCHEMA = 'FORECAST'
  AND PROCEDURE_NAME LIKE '%BUDGET%'
ORDER BY PROCEDURE_NAME;
```

## Step 2: Check Available Baselines

```sql
-- Find available FGMD baselines for budget generation
SELECT 
    pf.FORECAST_GENERATION_MONTH_DATE,
    MIN(pf.FORECAST_YEAR) as baseline_year,
    MAX(pf.FORECAST_YEAR) as max_year,
    COUNT(DISTINCT pf.MARKET_CODE) as market_count,
    COUNT(*) as total_rows
FROM FORECAST.DEPLETIONS_FORECAST_PUBLISHED_FORECASTS pf
JOIN FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS p 
    ON pf.PUBLICATION_ID = p.PUBLICATION_ID
WHERE p.PUBLICATION_STATUS = 'consensus'
GROUP BY 1
ORDER BY 1 DESC
LIMIT 5;

-- Select a test FGMD (use most recent from above)
SET TEST_FGMD = '2025-08-01'; -- Replace with actual FGMD
```

## Step 3: Test Budget Generation

```sql
-- Test 1: Generate budget for test cycle
CALL FORECAST.SP_GENERATE_BUDGET($TEST_FGMD, 'validation_user');

-- Verify generation results
SELECT 
    BUDGET_CYCLE_DATE,
    FORECAST_YEAR,
    FORECAST_METHOD,
    COUNT(*) as row_count,
    ROUND(SUM(CASE_EQUIVALENT_VOLUME), 2) as total_volume,
    COUNT(DISTINCT MARKET_CODE) as market_count,
    COUNT(DISTINCT VARIANT_SIZE_PACK_ID) as product_count
FROM FORECAST.DEPLETIONS_BUDGET_GENERATED
WHERE BUDGET_CYCLE_DATE = $TEST_FGMD
GROUP BY 1, 2, 3
ORDER BY 3, 2;

-- Test 2: Verify primary methods seeded
SELECT 
    BUDGET_CYCLE_DATE,
    FORECAST_METHOD,
    COUNT(*) as key_count
FROM FORECAST.DEPLETIONS_BUDGET_PRIMARY_METHOD
WHERE BUDGET_CYCLE_DATE = $TEST_FGMD
GROUP BY 1, 2
ORDER BY 2;
```

## Step 4: Test Read Operations

```sql
-- Test 3: Read budget data (primary only)
SELECT COUNT(*) as primary_row_count
FROM TABLE(
    FORECAST.UDTF_GET_DEPLETIONS_BUDGET(
        $TEST_FGMD,
        NULL::VARCHAR,
        ARRAY_CONSTRUCT(),
        ARRAY_CONSTRUCT(), 
        ARRAY_CONSTRUCT(),
        TRUE  -- primary only
    )
);

-- Test 4: Read specific data with filtering
SELECT 
    MARKET_ID,
    CUSTOMER_ID,
    VARIANT_SIZE_PACK_DESC,
    YEAR,
    MONTH,
    FORECAST_METHOD,
    IS_MANUAL_INPUT,
    CASE_EQUIVALENT_VOLUME
FROM TABLE(
    FORECAST.UDTF_GET_DEPLETIONS_BUDGET(
        $TEST_FGMD,
        'six_month',  -- specific method
        ARRAY_CONSTRUCT(),  -- all markets
        ARRAY_CONSTRUCT(),  -- all customers
        ARRAY_CONSTRUCT(),  -- all products
        FALSE  -- all methods
    )
)
WHERE YEAR = (SELECT MIN(FORECAST_YEAR) + 1 FROM FORECAST.DEPLETIONS_BUDGET_GENERATED WHERE BUDGET_CYCLE_DATE = $TEST_FGMD)
ORDER BY MARKET_ID, CUSTOMER_ID, VARIANT_SIZE_PACK_DESC, MONTH
LIMIT 10;

-- Test 5: Check approval status
SELECT FORECAST.UDF_IS_BUDGET_APPROVED($TEST_FGMD) as is_approved;
```

## Step 5: Test Edit Operations

```sql
-- Test 6: Batch save budgets with various scenarios
CALL FORECAST.SP_BATCH_SAVE_BUDGETS($$
[
  {
    "market_code": "REPLACE_WITH_ACTUAL_MARKET",
    "customer_id": "REPLACE_WITH_ACTUAL_CUSTOMER", 
    "variant_size_pack_id": "REPLACE_WITH_ACTUAL_VSP",
    "forecast_year": 2026,
    "month": 1,
    "manual_case_equivalent_volume": 100.0,
    "selected_forecast_method": "six_month",
    "comment": "Validation test edit"
  },
  {
    "market_code": "REPLACE_WITH_ACTUAL_MARKET",
    "variant_size_pack_id": "REPLACE_WITH_ACTUAL_VSP", 
    "forecast_year": 2026,
    "month": 2,
    "manual_case_equivalent_volume": 200.0,
    "comment": "Market-level validation edit"
  }
]
$$, $TEST_FGMD, 'validation_user');

-- Verify edits were saved
SELECT 
    MARKET_CODE,
    DISTRIBUTOR_ID,
    VARIANT_SIZE_PACK_ID,
    FORECAST_YEAR,
    MONTH,
    MANUAL_CASE_EQUIVALENT_VOLUME,
    COMMENT,
    UPDATED_BY_USER_ID,
    CURRENT_VERSION
FROM FORECAST.MANUAL_INPUT_DEPLETIONS_BUDGET
WHERE BUDGET_CYCLE_DATE = $TEST_FGMD
  AND UPDATED_BY_USER_ID = 'validation_user'
ORDER BY UPDATED_AT DESC;

-- Test 7: Verify version history
SELECT 
    VERSION_NUMBER,
    MARKET_CODE,
    VARIANT_SIZE_PACK_ID,
    FORECAST_YEAR,
    MONTH,
    MANUAL_CASE_EQUIVALENT_VOLUME,
    VERSIONED_AT,
    VERSIONED_BY_USER_ID
FROM FORECAST.MANUAL_INPUT_DEPLETIONS_BUDGET_VERSIONS
WHERE BUDGET_CYCLE_DATE = $TEST_FGMD
ORDER BY VERSIONED_AT DESC
LIMIT 5;

-- Test 8: Verify method-agnostic behavior
SELECT 
    FORECAST_METHOD,
    CASE_EQUIVALENT_VOLUME,
    IS_MANUAL_INPUT
FROM TABLE(
    FORECAST.UDTF_GET_DEPLETIONS_BUDGET(
        $TEST_FGMD,
        NULL::VARCHAR,
        ARRAY_CONSTRUCT('REPLACE_WITH_ACTUAL_MARKET'),
        ARRAY_CONSTRUCT('REPLACE_WITH_ACTUAL_CUSTOMER'),
        ARRAY_CONSTRUCT('REPLACE_WITH_ACTUAL_VSP'),
        FALSE
    )
)
WHERE YEAR = 2026 AND MONTH = 1
ORDER BY FORECAST_METHOD;
```

## Step 6: Test Approval Workflow

```sql
-- Test 9: Approve budget cycle
CALL FORECAST.SP_APPROVE_BUDGET($TEST_FGMD, 'validation_manager', 'Test approval', TRUE);

-- Verify approval status
SELECT FORECAST.UDF_IS_BUDGET_APPROVED($TEST_FGMD) as is_now_approved;

-- Check approval record
SELECT 
    BUDGET_CYCLE_DATE,
    APPROVED_BY_USER_ID,
    APPROVED_AT,
    APPROVAL_NOTE
FROM FORECAST.DEPLETIONS_BUDGET_APPROVALS
WHERE BUDGET_CYCLE_DATE = $TEST_FGMD;

-- Test 10: Verify edits blocked after approval
CALL FORECAST.SP_BATCH_SAVE_BUDGETS($$
[
  {
    "market_code": "REPLACE_WITH_ACTUAL_MARKET",
    "variant_size_pack_id": "REPLACE_WITH_ACTUAL_VSP",
    "forecast_year": 2026,
    "month": 3,
    "manual_case_equivalent_volume": 300.0,
    "comment": "Should fail - approved cycle"
  }
]
$$, $TEST_FGMD, 'validation_user');
-- Expected: This should return an error about approved cycle
```

## Step 7: Test Unlock (Optional)

```sql
-- Test 11: Unlock for emergency changes (admin function)
CALL FORECAST.SP_APPROVE_BUDGET($TEST_FGMD, 'validation_admin', 'Test unlock', FALSE);

-- Verify unlocked
SELECT FORECAST.UDF_IS_BUDGET_APPROVED($TEST_FGMD) as is_still_approved;

-- Test edit after unlock
CALL FORECAST.SP_BATCH_SAVE_BUDGETS($$
[
  {
    "market_code": "REPLACE_WITH_ACTUAL_MARKET",
    "variant_size_pack_id": "REPLACE_WITH_ACTUAL_VSP",
    "forecast_year": 2026,
    "month": 3,
    "manual_case_equivalent_volume": 300.0,
    "comment": "Should work - unlocked cycle"
  }
]
$$, $TEST_FGMD, 'validation_user');
```

## Step 8: Performance Validation

```sql
-- Test 12: Performance check - large dataset read
SELECT COUNT(*) as total_rows,
       AVG(CASE_EQUIVALENT_VOLUME) as avg_volume,
       execution_time_ms
FROM TABLE(
    FORECAST.UDTF_GET_DEPLETIONS_BUDGET(
        $TEST_FGMD,
        NULL::VARCHAR,
        ARRAY_CONSTRUCT(),
        ARRAY_CONSTRUCT(), 
        ARRAY_CONSTRUCT(),
        TRUE
    )
),
(SELECT DATEDIFF('millisecond', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()) as execution_time_ms);

-- Test 13: Check clustering effectiveness
SELECT 
    SYSTEM$CLUSTERING_INFORMATION('FORECAST.DEPLETIONS_BUDGET_GENERATED', 
                                  '(BUDGET_CYCLE_DATE, MARKET_CODE, FORECAST_METHOD)') 
    as clustering_info;
```

## Step 9: Cleanup Test Data

```sql
-- Clean up validation data (optional)
DELETE FROM FORECAST.MANUAL_INPUT_DEPLETIONS_BUDGET 
WHERE BUDGET_CYCLE_DATE = $TEST_FGMD 
  AND UPDATED_BY_USER_ID IN ('validation_user', 'validation_manager', 'validation_admin');

DELETE FROM FORECAST.DEPLETIONS_BUDGET_APPROVALS
WHERE BUDGET_CYCLE_DATE = $TEST_FGMD;

DELETE FROM FORECAST.DEPLETIONS_BUDGET_GENERATED
WHERE BUDGET_CYCLE_DATE = $TEST_FGMD;

-- Note: Version history is append-only and should not be deleted
```

## Expected Results Summary

✅ **All tests should pass without errors except where noted**
✅ **Read operations should return data in expected format** 
✅ **Edit operations should create versions and update current data**
✅ **Approval workflow should prevent edits when locked**
✅ **Method-agnostic edits should appear across all methods**
✅ **Performance should meet target thresholds (< 5s reads, < 2s batch writes)**

## Troubleshooting Failed Tests

1. **No baseline data**: Check that consensus forecasts exist for your test FGMD
2. **Permission errors**: Verify schema grants and role assignments
3. **Missing tables**: Run DDL deployment scripts
4. **Procedure not found**: Deploy stored procedures and functions
5. **JSON parsing errors**: Check JSON syntax and field names
6. **Lock conflicts**: Verify approval status and use unlock if needed

Replace all `REPLACE_WITH_ACTUAL_*` placeholders with real values from your environment before running the validation suite.