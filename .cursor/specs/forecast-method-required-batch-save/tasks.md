# Tasks: Forecast Method Required in Batch Save Budgets

- Feature: `forecast-method-required-batch-save`
- Owner: Data Platform
- Status: in-progress

## Tasks
- T-001
  - Summary: Update exception message text for `missing_required_fields_ex` to include `forecast_method` and allowed values
  - Links: R-003
  - Steps:
    - Edit procedure declaration line for exception message
  - Expected outcome: The raised message matches spec
  - Test hints: Intentionally trigger the check and assert message
  - Complexity: XS
  - Dependencies: None
  - Status: completed

- T-002
  - Summary: Add `forecast_method` presence and allowed-set validation in Pre-flight Check 2
  - Links: R-001, R-002, R-003
  - Steps:
    - Extend the WHERE clause to check `forecast_method` non-empty and in allowed set
  - Expected outcome: Invalid payloads fail fast with -20013
  - Test hints: Use JSON missing field and invalid value; expect -20013
  - Complexity: S
  - Dependencies: T-001
  - Status: completed

- T-003
  - Summary: Smoke QA in dev
  - Links: R-004
  - Steps:
    - Run procedure with valid sample JSON covering all allowed methods
    - Confirm SUCCESS message and MERGE occurs
  - Expected outcome: No regressions; valid payloads proceed
  - Test hints: Use minimal record set; verify success
  - Complexity: XS
  - Dependencies: T-001, T-002

## Status Log
- 2025-09-10: Implemented validation and message; ready for smoke QA
