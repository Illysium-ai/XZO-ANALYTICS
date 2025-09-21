# Tasks — Real-time Planned Product Sync

- T-001 (Spec): Create requirements/design/tasks under `.cursor/specs/real-time-planned-sync/` [Done]
- T-002 (SP): Implement `FORECAST.SP_SYNC_PLANNED_PRODUCTS_REALTIME(P_VARIANT_SIZE_PACK_IDS ARRAY, P_UPDATED_BY_USER_ID VARCHAR)`
  - Derive `V_FGMD` via `FORECAST.UDF_GET_VALID_FORECAST_GENERATION_MONTH_DATE()`
  - Derive `V_BUDGET` via `SELECT MAX(BUDGET_CYCLE_DATE) FROM FORECAST.DEPLETIONS_BUDGET_GENERATED`
  - Build `seed_universe` and `purge_universe`
  - Execute purges across forecast/budget/primary/ manual tables
  - Zero-seed budget + primary method
  - Zero-seed forecast drafts (±chains) + primary methods
  - Collect SQLROWCOUNT metrics; return summary string
- T-003 (Wiring): Update `MASTER_DATA.SP_BATCH_UPDATE_APOLLO_VARIANT_SIZE_PACK_TAGS`
  - Replace calls to `SP_REFRESH_FORECAST_ON_PLANNED_EXCLUSIONS` and `SP_SEED_ZERO_FOR_PLANNED_PRODUCTS_BUDGET` with a single call to the new SP
  - Pass `ARRAY_CONSTRUCT(<vsp_id>)` per affected item (post-commit) or batch accumulate per session
- T-004 (Observability): Create optional `FORECAST.SYNC_LOG` and instrument per-table metrics [Optional]
- T-005 (Perf): Validate runtime on 50 and 1000 VSPs in dev; adjust set-based operations if necessary
- T-006 (Verification):
  - AC-001..AC-007 validation queries
  - Idempotency rerun check (no-op inserts)
  - Exclusion semantics check (market and distributor arrays)
  - Chains coverage check when chain surface exists for FGMD
- T-007 (Rollout): Feature flag the single-call path; provide rollback doc to original two SP calls

Status
- T-001: completed
- T-002: completed
- T-003: completed
- T-004: optional
- T-005: in-progress
- T-006: todo
- T-007: todo
