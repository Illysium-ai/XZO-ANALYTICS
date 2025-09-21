# Budget Module Requirements (EARS)

This document defines the requirements for a new Budget Module that:
- Selects a "baseline" from `FORECAST.DEPLETIONS_FORECAST_PUBLISHED_FORECASTS` by a chosen `FORECAST_GENERATION_MONTH_DATE` (FGMD)
- Uses the baseline year's (Y) actuals + forecast months as inputs to compute anchors
- Generates the following year (Y+1) budgets only using the same 3/6/12-month, run_rate, and flat forecast logic used by `depletions_forecast_init_draft`
- Supports user edits with complete version history

## Scope

Markets: William Grant initial tenant (`APOLLO_WILLIAMGRANT`), designed to be multi-tenant compatible.
Schema: Primary artifacts in `FORECAST` schema unless noted.
Grain: Match published forecast grain: `market_code, distributor_id, variant_size_pack_id, forecast_year, month, forecast_method, forecast_generation_month_date`.

## Requirements

- R-001 (Baseline selection): The system shall allow selecting a Budget Baseline by filtering `FORECAST.DEPLETIONS_FORECAST_PUBLISHED_FORECASTS` on a supplied `FORECAST_GENERATION_MONTH_DATE` (FGMD).
  - Acceptance: Passing an FGMD returns only rows with that exact FGMD; changing FGMD changes the baseline deterministically.

- R-002 (Baseline year usage): The system shall use all months in the baseline year (Y)—including actuals and forecast months—as inputs to compute trend/run-rate anchors, but shall not include Y rows in the budget output.
  - Acceptance: Budget outputs contain only Y+1 months; Y is used solely for calculations.

- R-003 (Y+1 generation): The system shall generate months for year Y+1 using the same logic as `depletions_forecast_init_draft` (3/6/12-month trend, run_rate, flat) with identical business keys, and shall output only Y+1.
  - Acceptance: For a given key, generated Y+1 months exist for all 12 months; methods `three_month`, `six_month`, `twelve_month`, `run_rate`, and `flat` are present unless intentionally filtered by business rules. No Y months appear in outputs.

- R-004 (Anchor assumptions): The system shall anchor the generated Y+1 logic to an anchor date equivalent to end-of-year Y for historical windows (consistent with `latest_complete_month_date` handling in `depletions_forecast_init_draft`).
  - Acceptance: Rolling windows and run-rates use historical actuals through `EOMONTH(Dec Y)` when computing trend factors.

- R-005 (Method parity): The system shall maintain semantic parity with `depletions_forecast_init_draft` for: rolling sum windows, run-rate definition (e.g., 3-month average), flat = 1.0, and method fallback behavior.
  - Acceptance: Side-by-side comparison for a controlled sample shows equal results when using identical anchors and inputs.

- R-006 (Primary method): The system should determine a primary method for each key using existing logic (`depletions_forecast_primary_forecast_method` / `stg_hyperion__sku_forecast_method`), defaulting to six_month when undefined.
  - Acceptance: A column `is_primary_forecast_method` or equivalent contract is available, and downstream consumers can filter to a single method.

- R-007 (User edits): The system shall allow users to provide manual overrides at the same grain with fields `manual_case_equivalent_volume` and optional `comment`.
  - Acceptance: Overrides take precedence when present; a combined view returns final chosen volume per key with method annotation.

- R-008 (Version history): The system shall record every edit with a durable version history (who, when, what changed, comment, version number) and support revert-to-version.
  - Acceptance: Saving edits creates a new version; querying history shows chronological entries; revert restores exact previous values.

- R-009 (Immutability of baseline): The system shall not mutate the source published baseline; it shall persist edits and generated values separately (analytical model + hybrid tables) or via an auditable approach if unified.
  - Acceptance: No updates occur to `DEPLETIONS_FORECAST_PUBLISHED_FORECASTS`.

- R-010 (Approval lock): The system shall support approving a budget cycle (FGMD), which locks edits for that cycle.
  - Acceptance: An approval action records approver metadata and time, and prevents further writes for that FGMD until explicitly unlocked by an elevated operation (if supported).

- R-011 (APIs/contracts): The system shall expose read and write contracts: a UDTF to read the working set, and a stored procedure to batch-save edits and create versions.
  - Acceptance: Callable contracts exist in `FORECAST` schema; they validate inputs and enforce locking/approval rules.

- R-012 (Performance): The system should serve read queries for a single FGMD and market within 5 seconds under expected data volumes; write paths must remain responsive (<2 seconds per batch of 100 rows under normal load).
  - Acceptance: Load tests meet or exceed targets; indexes/cluster keys align to access paths.

- R-013 (Security): The system shall restrict write access to authorized roles; read access is tenant-scoped.
  - Acceptance: Grants are defined and verified; least-privilege defaults.

- R-014 (dbt integration): The system shall provide a dbt model for analytical consumption of the combined final budget (baseline carried + generated + manual overrides) with `ref(...)` compatibility.
  - Acceptance: Model builds successfully in CI, documented and tested with dbt tests (not null, uniqueness at business grain, referential integrity to dimension tables where applicable).

- R-015 (Observability): The system shall provide lightweight metrics: number of generated rows, number of overridden rows, method mix, and last refresh timestamps.
  - Acceptance: Metrics exposed via a small audit table or model; visible via simple SQL.

- R-016 (Method-agnostic edits): The system shall treat manual inputs as method-agnostic at the monthly grain. An edit to a given `(budget_cycle_date, market_code, distributor_id, variant_size_pack_id, forecast_year, month)` applies regardless of `forecast_method`.
  - Acceptance: When a user edits while viewing any method (e.g., six_month), the final value for that key is overridden for all methods in reads; writes do not require specifying a method.

- R-018 (Budget primary method management): The system shall seed a budget-scoped primary method per `(budget_cycle_date, market_code, distributor_id, variant_size_pack_id)` during generation using existing logic (fallback six_month), and allow users to update the budget primary method independently via batch saves; on approval, primary method becomes locked for that cycle.
  - Acceptance: Initial seed present post-generation; subsequent batch saves can set `selected_forecast_method` per key; reads respect budget primary method for primary-only views; post-approval attempts to change primary method are rejected.

- R-017 (Batch-only edits): The system shall support only batch edit saves for budgets; single-row save endpoints are not required.
  - Acceptance: Only `SP_BATCH_SAVE_BUDGETS` is provided for editing; attempts to call non-batch save paths are disallowed.

## Out of Scope
- Multi-year rolling budgets beyond Y and Y+1.
- UI implementation; this spec covers data contracts and backend.
- Cross-tenant sharding strategy (design supports it but not implemented here).

## Assumptions
- Historical actuals required for trend factors are already available to the project (as used by `depletions_forecast_init_draft`).
- The baseline FGMD is user-supplied and valid for the selected market(s).
