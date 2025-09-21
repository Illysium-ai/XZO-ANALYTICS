# Budget Generation — Anchor at P_BUDGET_CYCLE_DATE and Actuals-Based Trend Factors

## EARS Requirements
- R-001: WHEN generating a budget for a given `P_BUDGET_CYCLE_DATE`, THE SYSTEM SHALL set the anchor date equal to `P_BUDGET_CYCLE_DATE`.
- R-002: WHEN computing 3/6/12-month trend factors, THE SYSTEM SHALL use CY actuals up to and including the anchor date and PY actuals up to and including the anchor date minus one year.
- R-003: WHEN computing the run-rate factor, THE SYSTEM SHALL use the average of the most recent 3 months of CY actuals up to the anchor date.
- R-004: WHEN applying trend factors (3/6/12), THE SYSTEM SHALL apply the factor to all 12 months of the target year `Y+1` by scaling each corresponding month of CY published consensus volumes.
- R-005: WHEN generating the output months, THE SYSTEM SHALL generate months January through December of the target year `Y+1` regardless of the anchor month.
- R-006: WHEN the required CY published consensus baseline for `P_BUDGET_CYCLE_DATE` does not exist, THE SYSTEM SHALL raise `missing_baseline_ex` and abort.
- R-007: WHEN generating budgets, THE SYSTEM SHALL continue to populate `CY_*`, `PY_*`, and `RUN_RATE_3M` fields reflecting the values as-of the anchor date for traceability.
- R-008: WHEN seeding zero rows for planned products and primary methods, THE SYSTEM SHALL retain the current behavior.
- R-009: WHEN zero-seeding planned keys not produced by TFU, THE SYSTEM SHALL populate `PY_CASE_EQUIVALENT_VOLUME` by joining to the PF consensus baseline for the same cycle and mapped month.

## Assumptions
- CY refers to `EXTRACT(YEAR FROM P_BUDGET_CYCLE_DATE)`; target year is `Y+1`.
- CY/PY actuals are sourced from `VIP.RAD_DISTRIBUTOR_LEVEL_SALES`.
- CY published consensus volumes are sourced from `FORECAST.DEPLETIONS_FORECAST_PUBLISHED_FORECASTS` filtered to `P_BUDGET_CYCLE_DATE` and `consensus` publication.

## Acceptance Criteria
- A-001: Calling the procedure with `P_BUDGET_CYCLE_DATE='2025-07-01'` sets the anchor to `2025-07-01` and `PY` anchor to `2024-07-01`.
- A-002: Trend factors use CY actuals through Jul 2025 vs PY actuals through Jul 2024 (rolling 3/6/12 windows ending at the anchor month).
- A-003: Generated rows cover months `2026-01` through `2026-12`.
- A-004: For methods `three_month`, `six_month`, and `twelve_month`, `CASE_EQUIVALENT_VOLUME` equals CY 2025 published consensus volume for the same month multiplied by the single factor (constant across all 12 months for each method).
- A-005: For method `run_rate`, `CASE_EQUIVALENT_VOLUME` equals the 3-month average of CY actuals as-of the anchor date (applied uniformly across all 12 months).
- A-006: If no `consensus` baseline exists for `P_BUDGET_CYCLE_DATE`, the procedure raises `missing_baseline_ex`.
- A-007: For zero-seeded rows, `PY_CASE_EQUIVALENT_VOLUME` equals the PF consensus `CASE_EQUIVALENT_VOLUME` for CY and mapped month.

## Traceability
- R-001, R-002, R-003, R-004, R-005, R-006, R-007, R-008, R-009 → Implemented in `tenants/dbt_williamgrant/backend_functions/sf_budget_workflow/sp_generate_budget.sql`.
