# Budget Module Implementation Plan

Traces: R-001..R-018

Status legend: todo | in-progress | completed | blocked

## Phase 0 — Approval & Alignment
- T-000: Approve architecture and scope (this spec) — status: completed — outcome: approved by stakeholder

## Phase 1 — Foundational Storage & Contracts
- T-101: DDL — Create `FORECAST.MANUAL_INPUT_DEPLETIONS_BUDGET` (Hybrid, method-agnostic grain) — status: completed — links: R-007, R-008, R-013, R-016
  - Outcome: table created in `sf_budget_workflow/snowflake_budget_tables_ddl.sql` with composite key index.
- T-102: DDL — Create `FORECAST.MANUAL_INPUT_DEPLETIONS_BUDGET_VERSIONS` (Hybrid) — status: completed — links: R-008, R-013, R-016
  - Outcome: versions table created with lookup and join indexes.
- T-103: DDL — Create approval artifacts (simplified)
  - T-103a: `FORECAST.SP_APPROVE_BUDGET` — status: completed — links: R-010, R-013
    - Outcome: approval procedure created to lock cycle edits.
  - T-103b: `FORECAST.UDF_IS_BUDGET_APPROVED` — status: completed — links: R-010
    - Outcome: UDF created to check approval lock.
- T-104: UDTF — `FORECAST.UDTF_GET_DEPLETIONS_BUDGET` — status: completed — links: R-011, R-012, R-016
  - Outcome: UDTF created to return Y+1 working set with optional primary filter; uses view and method-agnostic precedence.
- T-105: SP — `FORECAST.SP_BATCH_SAVE_BUDGETS` (batch-only writes) — status: completed — links: R-007, R-008, R-011, R-016, R-017
  - Outcome: Validated batch writes with versioning, method-agnostic, enforces approval lock.
- T-106: SP — `FORECAST.SP_GENERATE_BUDGET` (FGMD-driven Y+1 generation, no dbt dependency) — status: completed — links: R-001, R-002, R-003, R-004, R-005, R-011
  - Outcome: Generates into `FORECAST.DEPLETIONS_BUDGET_GENERATED`; app triggers via Snowflake SP.
- T-107: DDL — Create `FORECAST.DEPLETIONS_BUDGET_PRIMARY_METHOD` and `..._VERSIONS` — status: completed — links: R-018, R-013
  - Outcome: Budget-scoped primary method with history; seeded during generation; user-editable until approval.
- T-108: SP — Extend `SP_BATCH_SAVE_BUDGETS` to accept `selected_forecast_method` and update budget primary method — status: completed — links: R-018, R-011
  - Outcome: Primary method updates piggyback with manual edits; versioned; respects approval lock.
- T-109: UDTF — Update `UDTF_GET_DEPLETIONS_BUDGET` to prefer budget primary method for primary-only reads — status: completed — links: R-018, R-011
  - Outcome: Primary-only view aligns with budget-specific choice (fallback to seed).

## Phase 2 — Analytical Models (dbt)
- T-201: dbt macro — extract trend factor & run-rate logic from `depletions_forecast_init_draft` to shared macro — status: todo — links: R-003, R-005
  - Outcome: not yet extracted; parity implemented directly in `depletions_budget_init` for now.
- T-202: dbt model — `depletions_budget_init` — status: completed — links: R-001, R-002, R-003, R-004, R-005, R-006
  - Outcome: Y as input-only anchor; Y+1-only outputs across methods; run_rate special-case; `budget_cycle_date` = selected FGMD.
- T-203: dbt view — `vw_depletions_budget_final` — status: completed — links: R-007, R-014, R-016
  - Outcome: Final merged budget (method-agnostic overrides); includes `comment` passthrough; documented via YAML.
- T-204: dbt sources/tests/docs — status: completed — links: R-014, R-015
  - Outcome: Forecast source YAML updated to include budget tables; new schema YAML with not_null tests for key columns.
- T-205: dbt audit model — `budget_audit` — status: todo — links: R-015
  - Outcome: Pending. To summarize generated vs overridden counts, method mix, last refresh per cycle/market.
- T-206: Complete dbt model documentation — status: completed — links: R-014
  - Outcome: `vw_get_budget_base` model implemented with proper source references and documentation.

## Phase 3 — Validation & QA
- T-301: Parity tests — ensure generated Y+1 matches `depletions_forecast_init_draft` semantics — status: in-progress — links: R-005
  - Outcome: Logic parity implemented; add unit queries to verify representative keys.
- T-302: Performance tests — FGMD+market read path under 5s; batch write path under 2s/100 rows — status: todo — links: R-012
  - Outcome: Pending. Requires env with target volumes.
- T-303: Security verification — roles & grants — status: todo — links: R-013
  - Outcome: Pending. Define grants for read/write/approve in deployment scripts.
- T-306: Method-agnostic override tests — ensure an edit applies to all methods for the same key — status: todo — links: R-016
  - Outcome: Pending. Add SQL checks on view/UDTF outputs after posting an override.
- T-307: Approval lock tests — verify no edits allowed post-approval, and that reads reflect approved state — status: todo — links: R-010
  - Outcome: Pending. Call `SP_APPROVE_BUDGET` then attempt batch save.

## Phase 4 — Enablement
- T-401: Documentation — user guides and SQL examples — status: completed — links: R-011, R-014, R-015
  - Outcome: Comprehensive API documentation created at `api_reference.md` with full procedure signatures, JSON schemas, error codes, and integration patterns for backend/frontend teams.
- T-402: CI integration — add to dbt job & basic tests — status: todo — links: R-014
  - Outcome: Pending. Wire into CI run list and ensure tests run green.
- T-403: API Reference Documentation — comprehensive backend/frontend integration guide — status: completed — links: R-011, R-014
  - Outcome: Created `api_reference.md` with complete API specifications, error handling, integration patterns, and troubleshooting guide for production use.

## Notes
- Keep changesets small; prefer additive DDL.
- Reuse established naming conventions and cluster/index strategy from existing forecast editing workflow.
- Ensure idempotent DDL and safe deploys across environments.
