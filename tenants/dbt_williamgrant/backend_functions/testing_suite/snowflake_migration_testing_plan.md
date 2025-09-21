# Snowflake Stored Procedure Migration: Comprehensive Test Plan

## 1. Introduction

This document outlines the testing strategy and plan for validating the successful migration of business logic from PostgreSQL functions to a new suite of optimized Snowflake Stored Procedures and UDFs. The goal is to ensure 100% functional parity, enforce new business rules, and confirm performance improvements.

The test suite is divided into three primary business workflows:
1.  **Manual Forecast Edits**
2.  **Forecast Publishing & Unpublishing**
3.  **Product Tagging**

## 2. Testing Methodology

The test suite is designed to be self-contained and repeatable. Each test script follows a consistent structure:

1.  **Setup:** The script begins by cleaning up any data from previous test runs to ensure a clean slate. It then inserts a known set of seed data required for the specific test scenarios.
2.  **Execution:** Each test case is clearly marked. It involves calling a specific Stored Procedure with defined parameters to simulate a user action.
3.  **Verification:** Following each call, one or more `SELECT` statements are provided. The results of these queries must be manually inspected to confirm the procedure had the expected effect on the database state (e.g., a status was updated, a new version was created, etc.).
4.  **Exception Handling Tests:** For scenarios that should be blocked by business rules (e.g., editing a published forecast), the test case will wrap the `CALL` in a `BEGIN...EXCEPTION` block. The test is considered successful if the expected exception is caught.

## 3. Test Suite Structure

The test scripts are located in `tenants/pg_func_migration/testing_suite/` and are designed to be run in order.

*   `test_01_manual_edits.sql`: Validates all scenarios related to creating, updating, and reverting manual forecast edits.
*   `test_02_publishing_workflow.sql`: A comprehensive script that tests the entire lifecycle of a forecast from 'draft' to 'review' to 'consensus', including the data sync and subsequent unpublishing actions.
*   `test_03_product_tagging.sql`: Validates the logic for adding and updating product tags for variant size packs.

## 4. Test Case Summary

### 4.1. Manual Edits

- **Objective:** Verify that users can save and manage manual edits, and that the system correctly versions these changes and prevents edits on locked forecasts.
- **Scenarios Covered:**
    - Initial save of a new manual forecast.
    - Updating an existing manual forecast.
    - Reverting a forecast to a previous version.
    - Reverting a forecast back to the automated "trend".
    - **Negative Test:** Ensure an exception is thrown when attempting to edit a forecast in a market that is already in 'review' or 'consensus' status.
    - Verify `sp_get_depletions_forecast` correctly displays the latest manual edits.

### 4.2. Publishing Workflow

- **Objective:** Verify the entire division-level publishing and unpublishing lifecycle.
- **Scenarios Covered:**
    - Publishing a division's forecasts to 'review'.
    - Promoting a specific division from 'review' to 'consensus'.
    - Verifying that a full consensus promotion triggers the data sync to the next forecast month.
    - Verifying that the global "valid forecast month" updates correctly.
    - Superuser promotion of all divisions to consensus.
    - Unpublishing a single market.
    - Unpublishing a publication group.
    - Unpublishing an entire division.
    - Verifying that unpublishing correctly reverts manual forecast statuses and the global "valid forecast month".

### 4.3. Product Tagging

- **Objective:** Verify that product tags can be correctly assigned and updated for products.
- **Scenarios Covered:**
    - Adding tags to a product for the first time.
    - Updating the tags for an existing product.
    - Creating a new tag in the master tag table by providing a previously unseen tag name.
    - Batch-updating tags for multiple products from a single JSON string. 