# Chains Publishing Workflow - Test Scripts

## Overview
This directory contains comprehensive test scripts for validating the chains publishing workflow implementation. The tests cover all aspects of the chains integration including batch saves, publishing, unpublishing, and data consistency.

**⚠️ Note: These test scripts use real data from the apollo_development database for realistic testing. The test data includes real markets USANY1 (Independent Franchise division) and USAPA1 (BBG and Control division), with chain codes 26097 and 50018, and variant size pack IDs TD001-12-750 and SJ001-12-750. The batch save tests work with both markets simultaneously, while publishing/unpublishing tests work with each division separately to validate division-level operations. Test cleanup preserves real master data while removing only the test forecast data created during testing.**

## Test Scripts

### `00_run_all_tests.sql` - **Master Test Runner**
- Executes all test scripts in proper sequence
- Includes error handling and test result summary
- Provides comprehensive test coverage report
- **Run this script to execute the complete test suite**

### `01_test_data_setup.sql` - **Test Data Setup**
- Uses real markets, divisions, and hierarchy data from apollo_development
- Sets up chains draft forecasts and primary methods using real data
- Creates manual chains forecasts (running table) with real identifiers
- Establishes core volume test data for comparison
- **Must be run first** or via the master runner

### `02_test_batch_save_chains.sql` - **Batch Save Testing**
- Tests `SP_BATCH_SAVE_FORECASTS_CHAINS` procedure
- Validates running table behavior (FR-004)
- Tests new record creation and existing record updates
- Validates version history tracking
- Tests primary method updates
- **Validates chains-specific running table behavior**

### `03_test_publishing_chains.sql` - **Publishing Testing**
- Tests chains publishing during consensus promotion
- Validates that chains do NOT publish during review (FR-003)
- Tests both manual and draft chains publishing
- Validates shared publication infrastructure (same group_id/publication_id)
- **Core publishing workflow validation**

### `04_test_unpublish_division.sql` - **Division Unpublishing**
- Tests `SP_UNPUBLISH_DIVISION_FORECAST` with chains integration
- Validates that both core and chains are unpublished together
- Tests atomic operations and data consistency
- Validates publication group status handling
- **Division-level unpublishing validation**

### `05_test_unpublish_market.sql` - **Market Unpublishing**
- Tests `SP_UNPUBLISH_MARKET_FORECAST` with chains integration
- Validates market-specific unpublishing
- Tests partial unpublishing scenarios (one market in division)
- Validates group status when all markets are unpublished
- **Market-level unpublishing validation**

### `06_test_cleanup.sql` - **Test Data Cleanup**
- Removes all test data created during testing (preserves real master data)
- Cleans published forecasts, publications, and manual data
- Validates complete cleanup with verification queries
- **Always run after testing to clean environment**

## Execution Instructions

### Quick Start (Recommended)
```sql
-- Execute complete test suite
!source 00_run_all_tests.sql;
```

### Manual Execution (for debugging specific tests)
```sql
-- Setup
!source 01_test_data_setup.sql;

-- Test specific functionality
!source 02_test_batch_save_chains.sql;
!source 03_test_publishing_chains.sql;
!source 04_test_unpublish_division.sql;
!source 05_test_unpublish_market.sql;

-- Cleanup
!source 06_test_cleanup.sql;
```

## Test Coverage

### ✅ **Functional Requirements Tested**
- **FR-001**: Chains consensus publishing ✓
- **FR-002**: Shared publication metadata ✓
- **FR-003**: No review-level chains publishing ✓
- **FR-004**: Running table behavior ✓
- **FR-005**: Chains division unpublishing ✓
- **FR-006**: Chains market unpublishing ✓
- **FR-007**: Publication infrastructure reuse ✓
- **FR-008**: Parent chain data handling ✓
- **FR-009**: Draft chains publishing ✓

### ✅ **Key Scenarios Tested**
- Batch save with running table behavior
- Consensus promotion triggering chains publishing
- Review publication NOT triggering chains publishing
- Manual and draft chains publishing together
- Division unpublishing removing both core and chains
- Market unpublishing with partial division scenarios
- Transactional consistency (all-or-nothing operations)
- No orphaned records after unpublishing
- Shared group_id and publication_id validation

### ✅ **Error Scenarios Tested**
- Invalid JSON validation in batch save
- Missing required fields validation
- Publication status edge cases
- Data consistency checks

## Expected Results

### ✅ **Success Indicators**
- All test scripts complete without errors
- All record counts match expected values
- No orphaned chains published forecasts exist
- Publication statuses are correct throughout workflow
- Cleanup script removes all test data

### ❌ **Failure Indicators**
- Procedure execution errors
- Incorrect record counts
- Orphaned chains records after unpublishing
- Missing chains records after consensus promotion
- Data integrity violations

## Test Environment

- **Database**: `APOLLO_DEVELOPMENT`
- **Schema**: `FORECAST`
- **Test Markets**: `USANY1` (Independent Franchise), `USAPA1` (BBG and Control)
- **Test Chains**: `26097` (USANY1), `50018` (USAPA1)
- **Test Variants**: `TD001-12-750` (USANY1), `SJ001-12-750` (USAPA1)
- **Test Duration**: ~5-10 minutes for complete suite

## Dependencies

### Required Procedures
- `SP_BATCH_SAVE_FORECASTS_CHAINS`
- `SP_PUBLISH_DIVISION_FORECAST` (with chains integration)
- `SP_UNPUBLISH_DIVISION_FORECAST` (with chains integration)
- `SP_UNPUBLISH_MARKET_FORECAST` (with chains integration)

### Required Tables
- `MANUAL_INPUT_DEPLETIONS_FORECAST_CHAINS`
- `DEPLETIONS_FORECAST_PUBLISHED_FORECASTS_CHAINS`
- `DEPLETIONS_FORECAST_INIT_DRAFT_CHAINS`
- `DEPLETIONS_FORECAST_PRIMARY_FORECAST_METHOD_CHAINS`
- All core volume publishing tables

## Notes

1. **Running Table Behavior**: Tests specifically validate that chains manual input table works as a running table (no FGMD field)

2. **Transactional Safety**: All tests verify that core and chains operations succeed or fail together

3. **Data Consistency**: Tests include extensive validation queries to ensure no orphaned or inconsistent data

4. **Performance**: Test data volumes are kept reasonable for quick execution while still validating functionality

5. **Cleanup**: Always run cleanup to avoid test data pollution between test runs