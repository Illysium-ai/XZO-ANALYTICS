# Changelog

## 2025-08-26

- Fix: Restore ability to clear tags for a `variant_size_pack_id` by passing an empty `tag_names` array in `MASTER_DATA.SP_BATCH_UPDATE_APOLLO_VARIANT_SIZE_PACK_TAGS`.
  - Behavior: The procedure now distinguishes between omitted `tag_names` (no change) and provided empty arrays (clear tags).
  - Example:

```sql
CALL APOLLO_DEVELOPMENT.MASTER_DATA.SP_BATCH_UPDATE_APOLLO_VARIANT_SIZE_PACK_TAGS(
  '[{"variant_size_pack_id":"RK001-96-50","tag_names": []},
    {"variant_size_pack_id":"BV007-6-750","tag_names": []}]'
);
```

- Note: Non-array `tag_names` values are ignored for tag updates; other fields still update.

## 2025-08-27

- Change: Make depletions manual overrides method-agnostic (align with budget workflow).
  - Write path:
    - `FORECAST.SP_BATCH_SAVE_FORECASTS` now upsserts by keys excluding method and sets `FORECAST_METHOD='MANUAL'`; duplicate detection ignores method; versions unaffected.
  - Read path:
    - `models/marts/forecast/vw_get_depletions_base.sql` now joins manual overrides by keys excluding method and adds `OVERRIDDEN_CASE_EQUIVALENT_VOLUME` + `IS_MANUAL_INPUT`.
  - Consensus sync:
    - `_INTERNAL_SP_SYNC_CONSENSUS_TO_NEXT_MONTH` dedupes across methods (prefers `'MANUAL'`, else latest `UPDATED_AT`) and writes `'MANUAL'` into next FGMD.
  - Chains:
    - Chains manual inputs already method-agnostic; no changes required.
  - Backward compatibility: No breaking DDL; unique constraint still includes method and will be revisited post-historical cleanup.

## 2025-09-10

- Feature: Budget generation aligned to PF-driven domain with calendarized RAD factors.
  - `FORECAST.SP_GENERATE_BUDGET`:
    - Anchor equals `P_BUDGET_CYCLE_DATE`; factors computed from RAD actuals using a 12‑month calendar ending at the anchor; applied across all 12 months of Y+1.
    - Domain now comes from PF consensus for the cycle (filtered by planned!=false, exclusions, and eligible distributors). Guarantees full PF coverage.
    - Backfill: Zero-seeded rows now populate `PY_CASE_EQUIVALENT_VOLUME` from PF for the same cycle/month.
  - Validation: For USANY1 / Balvenie / six_month, `PY` totals match PF within rounding.
  - Notes: Factor clamp remains 1.5; run_rate defined as CY 3-month average at anchor.

