# Tasks — Forecast Manual Inputs Method-Agnostic

Legend: status = todo | in-progress | completed | blocked

- T-001 (status: completed) — Repo scan and impact list
  - Links: R-108
  - Findings:
    - Write SP: `tenants/dbt_williamgrant/backend_functions/sf_forecast_editing_workflow/sp_batch_save_forecasts.sql`
      - Duplicate check/group-by removes `forecast_method`.
      - MERGE ON drops `FORECAST_METHOD`; set `FORECAST_METHOD='MANUAL'` on write; versions join ignores method or targets `'MANUAL'`.
    - Base view: `tenants/dbt_williamgrant/models/marts/forecast/vw_get_depletions_base.sql`
      - Manual join must ignore method. Add windowed dedupe per key to avoid row explosion until offline cleanup completes. Add `OVERRIDDEN_CASE_EQUIVALENT_VOLUME` and `IS_MANUAL_INPUT`.
    - Consensus sync SP: `tenants/dbt_williamgrant/backend_functions/sf_forecast_publishing_workflow/sp_internal_sync_consensus_to_next_month.sql`
      - Source dedupe across methods per key (prefer `'MANUAL'`, else latest `UPDATED_AT`). Target MERGE ignores method and sets `'MANUAL'`; versions append accordingly.
    - Chains (already method-agnostic for manual):
      - Write SP: `tenants/dbt_williamgrant/backend_functions/sf_forecast_editing_workflow/sp_batch_save_forecasts_chains.sql` — no change.
      - Base view: `tenants/dbt_williamgrant/models/marts/forecast/vw_get_depletions_base_chains.sql` — manual join already ignores method; no change.
    - DDL: `tenants/dbt_williamgrant/backend_functions/sf_forecast_editing_workflow/snowflake_forecast_editing_tables_ddl.sql`
      - Unique constraint currently includes `FORECAST_METHOD`. No DDL change now; tracked under T-999 (blocked).
    - Optional/ancillary (review if used operationally):
      - `tenants/dbt_williamgrant/analyses/backfill_sync_comments_next_fgmd.sql` — MERGE keys use `FORECAST_METHOD`.
      - `tenants/dbt_williamgrant/backend_functions/sf_product_tagging_workflow/sp_remap_variant_size_pack_id.sql` — ON includes `FORECAST_METHOD`.
  - Outcome: Confirmed impact list, no code changes.
  - Complexity: Low
  - Dependencies: None

- T-010 (status: completed) — Update `FORECAST.SP_BATCH_SAVE_FORECASTS`
  - Links: R-101, R-102, R-106, R-104
  - Steps:
    - Duplicate check: remove `forecast_method` from GROUP BY.
    - MERGE: drop method from ON keys; set `FORECAST_METHOD = 'MANUAL'` on INSERT/UPDATE.
    - Versions: ensure selection joins on normalized keys ignoring method.
    - Primary method table: keep update as-is (from payload).
    - Add tests for duplicate across methods and persistence across method switch.
  - Outcome: Single method-agnostic manual row per key; versions appended.
  - Complexity: Medium
  - Dependencies: T-001

- T-020 (status: completed) — Update `vw_get_depletions_base`
  - Links: R-103, R-108
  - Steps:
    - Remove method from manual LEFT JOIN condition.
    - Add derived `OVERRIDDEN_CASE_EQUIVALENT_VOLUME = COALESCE(m.MANUAL_CASE_EQUIVALENT_VOLUME, f.CASE_EQUIVALENT_VOLUME)` and `IS_MANUAL_INPUT` boolean.
    - Add windowed dedupe to select a single manual row per key until offline cleanup completes.
    - Keep existing columns for backward compatibility.
    - dbt compile/test.
  - Outcome: Method-agnostic read semantics.
  - Complexity: Low
  - Dependencies: T-001

- T-021 (status: completed) — Update `vw_get_depletions_base_chains`
  - Links: R-103, R-108
  - Steps:
    - Mirror changes from T-020 for chains view if needed (currently manual join already ignores method).
    - dbt compile/test.
  - Outcome: Chains read semantics aligned.
  - Complexity: Low
  - Dependencies: T-020

- T-030 (status: completed) — Update `_INTERNAL_SP_SYNC_CONSENSUS_TO_NEXT_MONTH`
  - Links: R-105, R-106
  - Steps:
    - Source: dedupe across methods per key using latest `UPDATED_AT` (prefer `'MANUAL'`).
    - Target MERGE: keys ignore method; set target `FORECAST_METHOD = 'MANUAL'`.
    - Versions: ensure single-row version appends.
  - Outcome: No duplicates created for next FGMD; versions correct.
  - Complexity: Medium
  - Dependencies: T-010

- T-050 (status: completed) — Regression and quality checks (compile-only per instruction)
  - Links: R-108
  - Steps:
    - Run dbt compile for impacted tenant (no runs).
    - Validate key user flows (manual checklist; no execution here).
    - Performance: scan joins/merges for obvious anti-patterns.
  - Outcome: Green compile and verified semantics.
  - Complexity: Medium
  - Dependencies: T-010, T-020, T-021, T-030

- T-060 (status: completed) — Docs and CHANGELOG
  - Links: R-108
  - Steps:
    - Update docs for manual override semantics.
    - Add CHANGELOG entry and PR description template.
  - Outcome: Documented change and release notes.
  - Complexity: Low
  - Dependencies: T-050

- T-999 (status: blocked) — Optional DDL hardening (post-ship)
  - Links: R-101
  - Steps:
    - Consider altering unique constraint to drop `FORECAST_METHOD` from the constraint.
    - Blocked pending offline historical cleanup; reindex/clustering review.
  - Outcome: Enforced method-agnostic uniqueness at storage.
  - Complexity: High
  - Dependencies: None
