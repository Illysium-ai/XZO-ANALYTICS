---
title: Tasks – Forecast Consolidation (Init Draft + Monthend)
status: draft
include: always
---

### Task List
- T-101 (Macro) – Create `forecast__monthend_projection` macro
  - Links: R-001, R-005, R-006, R-007
  - Steps:
    - Implement shared logic parameterized by grain ('distributor' | 'chain')
    - Inputs: source_model, target_year, target_month
    - Outputs: keys + projected_case_equivalent_quantity, projection_method_used
  - Status: todo

- T-102 (Non-chains model) – Integrate FGMD projection into `depletions_forecast_init_draft.sql`
  - Links: R-001, R-002, R-003, R-004, R-007
  - Steps:
    - Add `distributor_area` to `source_data` and propagate to necessary CTEs
    - Add `fgmd_prediction` CTE calling the macro for the FGMD month
    - Override FGMD row in `forecast_rows` using `CASE ...` with control-market exception
    - Replicate across all `forecast_method` values (unchanged behavior)
  - Status: todo

- T-103 (Chains model) – Integrate FGMD projection into `depletions_forecast_init_draft_chains.sql`
  - Links: R-001, R-002, R-003, R-004, R-005, R-007
  - Steps:
    - Add `distributor_area` from `rad_invoice_level_sales` and propagate
    - Add `fgmd_prediction` CTE (chain grain) and FGMD override with control exception
  - Status: todo

- T-104 (Standalone tables) – Refactor monthend prediction models to call the macro
  - Links: R-006, R-007
  - Steps:
    - Replace current body with thin wrapper calling the macro per month(s)
    - Preserve current outputs and diagnostics (`projection_method_used`)
  - Status: todo

- T-105 (Config) – Add variables and defaults
  - Links: R-002, R-008
  - Steps:
    - `var('control_distributor_areas', ['Control Market'])`
    - `var('include_projection_diagnostics', false)`
  - Status: todo

- T-106 (Tests) – Add validation tests
  - Links: R-010
  - Steps:
    - Assert FGMD equals projection for non-control markets
    - Assert FGMD equals legacy logic for control markets
  - Status: todo

- T-107 (Jobs) – Document mid-month refresh
  - Links: R-009
  - Steps:
    - Orchestration doc: `snow sql` + `dbt run --select <init-draft*>` refreshes the current FGMD by default; use `--vars '{"backfill_fgmd":"YYYY-MM-01"}'` to target a specific FGMD
  - Status: todo

### Notes
- Keep changesets small and verified per model; run `dbt test` after each integration.
- No breaking schema changes; diagnostics stay internal/optional.

### References
- #[[file:apollo-analytics/tenants/dbt_williamgrant/models/marts/forecast/depletions_forecast_init_draft.sql]]
- #[[file:apollo-analytics/tenants/dbt_williamgrant/models/marts/forecast/depletions_forecast_init_draft_chains.sql]]
- #[[file:apollo-analytics/tenants/dbt_williamgrant/models/marts/forecast/depletions_forecast_monthend_prediction.sql]]
- #[[file:apollo-analytics/tenants/dbt_williamgrant/models/marts/forecast/depletions_forecast_monthend_prediction_chains.sql]]
