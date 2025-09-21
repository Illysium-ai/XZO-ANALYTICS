# Budget Module Design

Traces: R-001, R-002, R-003, R-004, R-005, R-006, R-007, R-008, R-009, R-010, R-011, R-012, R-013, R-015

## Overview
Create a Budget Module that:
- Selects a published forecast baseline by `FORECAST_GENERATION_MONTH_DATE` (FGMD)
- Uses the baseline year (Y) as input-only for anchors (actuals + forecast months)
- Generates year Y+1 budgets only using the same methods as `depletions_forecast_init_draft` (three_month, six_month, twelve_month, run_rate, flat)
- Supports method-agnostic user overrides with full version history, and a simplified approve/lock

The design reuses existing forecasting assets and editing/versioning patterns while isolating budget-specific storage and contracts to avoid coupling.

## Key Concepts
- Baseline FGMD: User-chosen snapshot from `FORECAST.DEPLETIONS_FORECAST_PUBLISHED_FORECASTS`.
- First Year (Y): `min(forecast_year)` in the baseline; months Jan–Dec Y are carried as-is.
- Generated Year (Y+1): 12 months produced by logic with the same semantics as `depletions_forecast_init_draft`.
- Budget Cycle Date: Alias of FGMD used to link all derived/generated rows and user edits within a single budget cycle.

## Data Model

### New Analytical Models (dbt)
- `depletions_budget_init` (incremental/delete+insert; clustered by `budget_cycle_date, market_code, forecast_method`)
  - Inputs (Baseline): `FORECAST.DEPLETIONS_FORECAST_PUBLISHED_FORECASTS` filtered by FGMD → use year Y only to compute anchors; do not output Y.
  - Generated Output: produce 12 months for Y+1 across methods.
  - Output grain: matches published forecasts with added columns: `budget_cycle_date` (FGMD), `data_source = 'logic_driven'`.
  - Method parity: three_month, six_month, twelve_month, run_rate, flat.
  - Primary method: join to `depletions_forecast_primary_forecast_method` or `stg_hyperion__sku_forecast_method` for `is_primary_forecast_method` (default six_month). UI shows primary by default; users can switch method per row; manual override applies regardless of method.

- `vw_get_budget_base` (view)
  - Combines `depletions_budget_init` with manual overrides (method-agnostic) to produce the "final" budget values.
  - Precedence: manual override when present else logic/published value. Overrides apply across all methods by joining on keys excluding `forecast_method`.

### New Hybrid Tables (transactional)
Schema: `FORECAST` (consistent with existing forecast editing tables)

- `DEPLETIONS_BUDGET_PRIMARY_METHOD` (Hybrid)
  - Purpose: Budget-scoped primary method per key, decoupled from core forecast primary.
  - Keys: `(BUDGET_CYCLE_DATE, MARKET_CODE, DISTRIBUTOR_ID, VARIANT_SIZE_PACK_ID)`
  - Columns: `FORECAST_METHOD`, `UPDATED_BY`, `UPDATED_AT`; seed during generation (fallback six_month), then user-editable until approval

- `DEPLETIONS_BUDGET_PRIMARY_METHOD_VERSIONS` (Hybrid, append-only)
  - Purpose: Immutable history of primary method changes per key and cycle
  - Columns: business keys above + `VERSION_ID`, `VERSION_NUMBER`, `FORECAST_METHOD`, `VERSIONED_AT`, `VERSIONED_BY`, `COMMENT`

- `MANUAL_INPUT_DEPLETIONS_BUDGET` (Hybrid)
  - Purpose: Current-state overrides for budget at method-agnostic monthly grain.
  - Keys: `(budget_cycle_date, market_code, distributor_id, variant_size_pack_id, forecast_year, month)`
  - Columns (subset):
    - `BUDGET_CYCLE_DATE` DATE (FGMD)
    - `MARKET_CODE` VARCHAR
    - `DISTRIBUTOR_ID` VARCHAR
    - `VARIANT_SIZE_PACK_ID` VARCHAR
    - `FORECAST_YEAR` NUMBER
    - `MONTH` NUMBER (1–12)
    - `MANUAL_CASE_EQUIVALENT_VOLUME` FLOAT
    - `COMMENT` TEXT NULL
    - `UPDATED_BY` VARCHAR, `UPDATED_AT` TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP

- `MANUAL_INPUT_DEPLETIONS_BUDGET_VERSIONS` (Hybrid, append-only)
  - Purpose: Immutable history of every change.
  - Columns: All business keys above + `VERSION_ID` (UUID), `VERSION_NUMBER` (int), `VERSIONED_AT`, `VERSIONED_BY`, `ACTION` (insert/update/delete), `PREV_VALUE`, `NEW_VALUE`, `COMMENT`.

- Optional (publish/freeze):
  - `DEPLETIONS_BUDGET_PUBLICATIONS` (Hybrid)
  - `DEPLETIONS_BUDGET_PUBLISHED` (Hybrid, append-only snapshot of the final budget at publish time)

### Source/Docs
- Add sources to `staging/forecast/_forecast__sources.yml` or a new `_budget__sources.yml` for budget tables.
- Document and test with dbt.

## Generation Logic (Y+1)
- Anchor: `latest_complete_month_date = EOMONTH(Dec Y)`
- Rolling windows: replicate `depletions_forecast_init_draft` windows (3/6/12 months) and bounds (e.g., `least(..., 1.5)` caps) where applicable.
- Run rate: 3-month average definition as in current model.
- Flat: multiplier 1.0.
- Previous consensus injection: not required for budget generation (optional enhancement), because baseline already comes from a published snapshot.
- Primary method assignment: same logic as forecasts; default to six_month when undefined.

To avoid duplication, extract shared calculations into a dbt macro, e.g., `macros/forecast/calc_trend_factors.sql`, reusable by both `depletions_forecast_init_draft` and `depletions_budget_init`.

## Read/Write Contracts (R-011, R-018)

- UDTF: `FORECAST.UDTF_GET_DEPLETIONS_BUDGET(
    P_BUDGET_CYCLE_DATE DATE,
    P_MARKET_CODE VARCHAR DEFAULT NULL,
    P_DISTRIBUTOR_ID VARCHAR DEFAULT NULL,
    P_PARENT_CHAIN_CODE VARCHAR DEFAULT NULL,
    P_VARIANT_SIZE_PACK_ID VARCHAR DEFAULT NULL,
    P_METHOD VARCHAR DEFAULT NULL,
    P_ONLY_PRIMARY BOOLEAN DEFAULT TRUE
  )`
  - Returns the combined working set (Y+1 only) for the specified cycle
  - Implements override precedence method-agnostically by left joining overrides on keys excluding `FORECAST_METHOD`; optional `P_METHOD` filter is for display only
  - Primary-only behavior: prefer `DEPLETIONS_BUDGET_PRIMARY_METHOD`; if not set, fallback to seeded default (derived at generation)

- SP (batch save only): `FORECAST.SP_BATCH_SAVE_BUDGETS(
    P_BUDGET_CYCLE_DATE DATE,
    P_BUDGETS_JSON VARIANT,
    P_USER_ID VARCHAR
  )`
  - Mirrors existing batch save patterns; enforces method-agnostic write grain; performs versioning per row
  - Accept optional `selected_forecast_method` per record; update `DEPLETIONS_BUDGET_PRIMARY_METHOD` for the key when provided (no volume required)
  - Version primary method changes in `..._VERSIONS`

- SP (primary method management):
  - `FORECAST.SP_SET_BUDGET_PRIMARY_METHOD(P_BUDGET_CYCLE_DATE, P_MARKET_CODE, P_DISTRIBUTOR_ID, P_VARIANT_SIZE_PACK_ID, P_FORECAST_METHOD, P_USER_ID)` (optional if batch route is sufficient)

- SP (versioning):
  - `FORECAST.SP_REVERT_BUDGET_TO_VERSION(P_VERSION_ID UUID)` to restore prior values

- Approval/Lock (simplified):
  - `FORECAST.SP_APPROVE_BUDGET(P_BUDGET_CYCLE_DATE DATE, P_APPROVED_BY VARCHAR, P_COMMENT TEXT)`
    - Records approval metadata and locks future edits for that FGMD
  - `FORECAST.UDF_IS_BUDGET_APPROVED(P_MARKET_CODE, P_BUDGET_CYCLE_DATE)` to check lock state
  - `FORECAST.SP_APPROVE_BUDGET(P_BUDGET_CYCLE_DATE DATE, P_APPROVED_BY VARCHAR, P_COMMENT TEXT)` locks both manual inputs and `DEPLETIONS_BUDGET_PRIMARY_METHOD` for the cycle

## Flow

```mermaid
flowchart TD
  A[Select FGMD as Budget Cycle Date] --> B[dbt model depletions_budget_init]
  B -->|Generated Y+1 via 3/6/12/run_rate/flat| C[Budget Working Set]
  C --> D[UDTF_GET_DEPLETIONS_BUDGET]
  D --> E[Client/UI (Primary default, switchable)]
  E --> F[SP_BATCH_SAVE_BUDGETS]
  F --> G[MANUAL_INPUT_DEPLETIONS_BUDGET]
  F --> H[MANUAL_INPUT_DEPLETIONS_BUDGET_VERSIONS]
  C --> I[vw_depletions_budget_final]
  G --> I
  I --> J[Analytics/Reporting]
  I --> K[SP_APPROVE_BUDGET]
  K --> L[Approved (locked) state]
```

## Partitioning, Clustering, and Indexing (R-012)
- Analytical models: `cluster_by = ['budget_cycle_date', 'market_code', 'forecast_method']` to match typical filters and joins.
- Hybrid tables: Secondary indexes on `(MARKET_CODE, DISTRIBUTOR_ID, VARIANT_SIZE_PACK_ID, FORECAST_YEAR, MONTH, BUDGET_CYCLE_DATE)`; override is method-agnostic.

## Security (R-013)
- Create dedicated roles for budget read/write.
- Restrict publish/revert operations to elevated roles.

## Observability (R-015)
- `BUDGET_AUDIT` (analytical model) summarizing counts: generated rows, overridden rows, method mix, and last refresh timestamps per cycle and market.

## Alternatives Considered
- Reusing `MANUAL_INPUT_DEPLETIONS_FORECAST` with a `data_context = 'budget'` column.
  - Trade-off: Fewer tables but higher risk of coupling and accidental cross-context edits. Rejected for clarity and safety.
- Generating Y+1 solely from published values without historical actuals.
  - Trade-off: Simpler but breaks parity with `depletions_forecast_init_draft` and business semantics. Rejected.

## Risks & Mitigations
- Risk: Divergence between forecast logic and budget logic.
  - Mitigation: Extract a shared macro for factor calculations and unit test parity.
- Risk: Performance on large markets.
  - Mitigation: Proper clustering and secondary indexes; ensure dbt incremental strategies match usage.
- Risk: Edit contention and locking.
  - Mitigation: Reuse locking/published checks from forecast workflows; enforce at SP layer.
