# Real-time Planned Product Sync (Unified Purge + Seed SP)

## Requirements (EARS)

- R-001: WHEN a VSP’s planning flags or exclusions change via batch update, THE SYSTEM SHALL purge all non-eligible forecast and budget artifacts for those VSPs in the current FGMD and budget cycle.
- R-002: WHEN a VSP is planned and not excluded for a market/distributor that is depletions-eligible, THE SYSTEM SHALL zero-seed Budget generated rows for the active budget cycle where they do not exist.
- R-003: WHEN a VSP is planned and not excluded, THE SYSTEM SHALL zero-seed Forecast init drafts (distributor and chains) for the valid FGMD across 12 months and five forecast methods (three_month, six_month, twelve_month, flat, run_rate), where missing.
- R-004: WHEN zero-seeding forecast rows, THE SYSTEM SHALL set data_source to 'zero_seeded' and set volumes to 0.0.
- R-005: WHEN seeding forecast rows, THE SYSTEM SHALL seed or upsert primary forecast methods with default 'six_month' when absent for both distributor-level and chain-level.
- R-006: WHEN seeding Budget rows, THE SYSTEM SHALL also seed missing primary methods with default 'six_month' for the same cycle.
- R-007: THE SYSTEM SHALL derive the valid forecast generation month date (FGMD) using `FORECAST.UDF_GET_VALID_FORECAST_GENERATION_MONTH_DATE()`.
- R-008: THE SYSTEM SHALL derive the current budget cycle date as the maximum `BUDGET_CYCLE_DATE` from `FORECAST.DEPLETIONS_BUDGET_GENERATED`.
- R-009: THE SYSTEM SHALL ensure idempotency: re-running the SP for the same inputs must not create duplicates.
- R-010: THE SYSTEM SHALL operate set-based (no per-row loops) for performance and scalability.
- R-011: THE SYSTEM SHALL complete within 5 seconds for up to 50 VSPs and within 60 seconds for up to 1,000 VSPs under normal load.
- R-012: THE SYSTEM SHALL emit per-table row-change metrics for observability and return a concise status message.
- R-013: THE SYSTEM SHALL be wired as the single post-commit hook from `MASTER_DATA.SP_BATCH_UPDATE_APOLLO_VARIANT_SIZE_PACK_TAGS` so user actions are visible near real-time.
- R-014: THE SYSTEM SHALL limit zero-seeding scope to depletions-eligible distributors (`CORE/APOLLO_DIST_MASTER.IS_DEPLETIONS_ELIGIBLE = 1`).
- R-015: THE SYSTEM SHALL respect tag-based market and distributor exclusions and `IS_PLANNED = FALSE` semantics for purges and for excluding seed coverage.

## Acceptance Criteria

- AC-001: After a batch update that sets VSPs to planned with no exclusions, forecast init drafts (and chains) contain 12 × 5 seeded rows per market/distributor pair within the valid FGMD; `data_source = 'zero_seeded'` and `case_equivalent_volume = 0.0`.
- AC-002: After the same update, budget generated contains 12 seeded rows per market/distributor for the active cycle where previously absent; primary methods exist or are seeded with 'six_month'.
- AC-003: Setting a VSP to unplanned or applying exclusions deletes affected rows across forecast init drafts (+chains), manual input current/versions (forecast and budget), budget generated, and forecast/budget primary method tables for the active FGMD/cycle.
- AC-004: Re-running the SP immediately produces no additional inserts (idempotency verified) and reports zero or expected row deltas.
- AC-005: The SP returns a summary string and logs metrics (per-table affected row counts) for each invocation.
- AC-006: Manual and published forecasts are unaffected by seeding; publishing workflow remains unchanged.
- AC-007: Changes are visible to end users without waiting for dbt runs; downstream dashboards reflect zero-seeded data immediately.
