---
title: Forecast Init Draft + Monthend Prediction Consolidation
status: draft
include: always
# file references used across the spec for quick navigation
references:
  - "#[[file:apollo-analytics/tenants/dbt_williamgrant/models/marts/forecast/depletions_forecast_init_draft.sql]]"
  - "#[[file:apollo-analytics/tenants/dbt_williamgrant/models/marts/forecast/depletions_forecast_init_draft_chains.sql]]"
  - "#[[file:apollo-analytics/tenants/dbt_williamgrant/models/marts/forecast/depletions_forecast_monthend_prediction.sql]]"
  - "#[[file:apollo-analytics/tenants/dbt_williamgrant/models/marts/forecast/depletions_forecast_monthend_prediction_chains.sql]]"
---

### Overview
Unify the current init-draft forecasting models with the month-end prediction logic so that, for the forecast generation month date (FGMD), the forecast uses a consistent, data-to-date projection rather than PY*trend/run-rate. Control markets (identified via `distributor_area`) continue to use the existing init-draft logic.

### Definitions
- FGMD: forecast_generation_month_date (e.g., 2025-08-01 for August 2025)
- Month-end projection: logic that uses actuals-to-date in the current month and projects to a full month via factor-based or straight-line business-day methods (see monthend prediction models).
- Control markets: records whose `distributor_area` belongs to a configurable allowlist, evaluated from the RAD facts:
  - Chains: `rad_invoice_level_sales`
  - Non-chains: `rad_distributor_level_sales`

### Requirements (EARS)
- R-001 (Core FGMD projection): When generating init-draft forecasts, the system shall compute the FGMD month’s volume using the month-end projection logic based on actuals-to-date for that same month.
  - Acceptance: FGMD month rows in init_draft and init_draft_chains match the projection produced by month-end logic at the same grain.

- R-002 (Control market exception): When a record belongs to a control market (via `distributor_area`), the system shall bypass month-end projection and use the existing init-draft method for the FGMD month.
  - Acceptance: For control markets, FGMD month values remain identical to current/init-draft behavior.

- R-003 (Scope-limited projection): The system shall only apply month-end projection to the FGMD month; all months after FGMD shall continue to use trend-factor or run-rate methods.
  - Acceptance: Months after FGMD are unaffected; historic months remain as `actual_complete`.

- R-004 (Shape compatibility): The system shall preserve the current output schemas of init-draft models (column names, types, meaning), including replicating FGMD values across each `forecast_method` like current actuals are replicated.
  - Acceptance: No downstream model changes required; CI tests for schema pass unchanged.

- R-005 (Dual-grain parity): The system shall support both distributor-level and chain-level grains without duplicating logic.
  - Acceptance: Chains and non-chains produce consistent FGMD behavior with aligned logic; only grouping keys differ.

- R-006 (Reusability): The system shall extract the month-end projection into a reusable component (macro/CTE pattern) that can power both init-draft models and keep the existing monthend prediction tables as thin wrappers.
  - Acceptance: A single macro (or pair with a flag) drives both the consolidated FGMD logic and the standalone prediction tables.

- R-007 (Performance guardrails): The system shall restrict projection calculations to the FGMD month (not past three months) when called from init-draft, to minimize compute.
  - Acceptance: Explainable reduction in scan/compute vs. running a 3-month window inside init-draft.

- R-008 (Configurability): The system shall provide `var('control_distributor_areas', [...])` to define control markets and `var('include_projection_diagnostics', false)` to optionally include diagnostic columns during development only.
  - Acceptance: Default values produce identical public schema; enabling diagnostics adds transient, non-selected fields.

- R-009 (Refresh and backfill): By default, an incremental run shall refresh the current FGMD mid-month (delete+insert on the max FGMD). The `backfill_fgmd` var shall be used only to target a specific FGMD (e.g., a past month) when needed.
  - Acceptance: Running without vars re-generates the current FGMD; providing `backfill_fgmd` re-generates the specified FGMD partition.

- R-010 (Testability): The system shall include tests validating that FGMD month equals month-end projection for non-control markets and equals existing logic for control markets.
  - Acceptance: dbt tests or SQL checks covering representative segments pass in CI.

### Out of Scope
- Changing materialization of init-draft models or altering unique keys.
- Introducing mid-month automatic re-materialization schedules (can be handled by orchestration with `backfill_fgmd`).

### References
- Init-draft (non-chains): #[[file:apollo-analytics/tenants/dbt_williamgrant/models/marts/forecast/depletions_forecast_init_draft.sql]]
- Init-draft (chains): #[[file:apollo-analytics/tenants/dbt_williamgrant/models/marts/forecast/depletions_forecast_init_draft_chains.sql]]
- Monthend prediction (non-chains): #[[file:apollo-analytics/tenants/dbt_williamgrant/models/marts/forecast/depletions_forecast_monthend_prediction.sql]]
- Monthend prediction (chains): #[[file:apollo-analytics/tenants/dbt_williamgrant/models/marts/forecast/depletions_forecast_monthend_prediction_chains.sql]]
