### Task Plan

- T-001: Add `IS_CUSTOM_PRODUCT` to `MASTER_DATA.APOLLO_VARIANT_SIZE_PACK_TAG` and defaults  
  Links: R-001, R-002, R-003  
  Steps:
  - Alter Hybrid table (or recreate CTAS + ALTER) to add column with default false not null.
  - Backfill existing rows to false.
  - Update dbt model `marts/master/apollo_variant_size_pack_tag.sql` to include the column.  
  Outcome: Column present with default; dbt tests adjusted.  
  Status: done

- T-002: Extend SP `SP_BATCH_UPDATE_APOLLO_VARIANT_SIZE_PACK_TAGS` for flags/exclusions/custom inserts  
  Links: R-004, R-006, R-005  
  Steps:
  - Accept new JSON fields; allow INSERT without SKU master when `is_custom_product = true` and desc provided.
  - Implement partial updates for provided fields; keep existing tag logic.
  - Add audit logging.  
  Outcome: Upserts support full payload; errors descriptive.  
  Status: done

- T-003: Zero coverage in forecast init draft models (dbt)  
  Links: R-007  
  Steps:
  - In `depletions_forecast_init_draft.sql` and `_chains`, union zero rows for planned products where no historical depletions support logic-driven forecasts, across all methods and next 12 months.
  - Respect exclusions and `is_planned` via joins to `MASTER_DATA.APOLLO_VARIANT_SIZE_PACK_TAG` source.
  - Ensure incremental logic remains idempotent.  
  Outcome: Forecast models include planned, non-excluded zero rows.  
  Status: pending

- T-004: Ensure distributor allocation gap-filling covers planned products  
  Links: R-008  
  Steps:
  - Update `distributor_allocation_by_market_size_pack.sql` (and chains variant if applicable) to include planned product combos from forecast init draft and even distributions when missing.
  - Validate percentages sum to 1.0.  
  Outcome: Planned products receive allocations even without history.  
  Status: pending

- T-005: Zero coverage in budget generated and default primary method (SP + existing SP integration)  
  Links: R-009, R-010  
  Steps:
  - Extend `FORECAST.SP_GENERATE_BUDGET` to MERGE missing planned product rows (non-excluded) with zero volumes into `DEPLETIONS_BUDGET_GENERATED` for the cycle.
  - Ensure `DEPLETIONS_BUDGET_PRIMARY_METHOD` inserts default method (`six_month`) for any planned combo missing a record.
  - Keep operations idempotent per cycle date.  
  Outcome: Budget outputs include planned zeros and have primary methods.  
  Status: pending

- T-006: Forecast primary method defaults (dbt)  
  Links: R-010  
  Steps:
  - Confirm `depletions_forecast_primary_forecast_method.sql` includes planned zero-seeded combos via join to forecast init draft and default to `six_month` if missing config.
  - Ensure chains variant mirrors this behavior.  
  Outcome: Defaults present for planned combos.  
  Status: pending

- T-007: Implement `MASTER_DATA.SP_REMAP_VARIANT_SIZE_PACK_ID` (bulk update)  
  Links: R-011, R-012, R-013, R-014, R-017  
  Steps:
  - Validate target ID; optional dry-run to compute counts.
  - For each table, UPDATE with conflict handling; purge/regenerate generated tables.
  - Wrap transactional; write audit by table.  
  Outcome: One-call remap with counts and safety.  
  Status: done

- T-008: Tests and observability  
  Links: R-013, R-016, R-017  
  Steps:
  - Test directly via Snowflake calls and dbt runs; write test scripts in `backend_functions/testing_suite`.
  - Performance tests at target scale.
  - Dashboards for audit tables.  
  Outcome: Quality gates and telemetry.  
  Status: in_progress

- T-009: Documentation and runbooks  
  Links: R-014  
  Steps:
  - Update README and docs with procedures, payload examples, and dry-run guidance.
  - Create operator runbook for zero-coverage and remap workflows and rollbacks.  
  Outcome: Team alignment and operations ready.  
  Status: in_progress
