# Real-time Planned Product Sync — Design

## Overview
A single stored procedure `FORECAST.SP_SYNC_PLANNED_PRODUCTS_REALTIME` consolidates the purge and zero-seed flows across forecast and budget domains to provide near real-time visibility after user updates to product planning tags.

## Anchors and Scope
- Valid FGMD: obtained via `FORECAST.UDF_GET_VALID_FORECAST_GENERATION_MONTH_DATE()`
- Current budget cycle: `SELECT MAX(BUDGET_CYCLE_DATE) FROM FORECAST.DEPLETIONS_BUDGET_GENERATED`
- VSP scope: the input array of `VARIANT_SIZE_PACK_ID` values changed by `SP_BATCH_UPDATE_APOLLO_VARIANT_SIZE_PACK_TAGS`

## Data Flow (ASCII)

VSP changes → tag table state
  ├─ derive FGMD (UDF)
  ├─ derive current budget cycle (MAX in budget_generated)
  ├─ build SEED_SET (planned, eligible, non-excluded)
  ├─ build PURGE_SET (unplanned or excluded)
  │   └─ delete across forecast init (±chains), manual inputs, budget generated, primary methods
  └─ zero-seed across:
      • Budget_generated + Budget_primary_method (idempotent)
      • Forecast_init_draft + Forecast_init_draft_chains (12×5 methods, idempotent)
      • Forecast primary methods (±chains) with default 'six_month' if missing

## Contracts (tables/columns)
- Forecast init draft
  - Columns required for insert (subset shown): market_name, market_code, distributor_name, distributor_id, brand, brand_id, variant, variant_id, variant_size_pack_desc, variant_size_pack_id, forecast_year, month, forecast_month_date, forecast_method, case_equivalent_volume, py_case_equivalent_volume, data_type, trend_factor, latest_complete_month_date, forecast_generation_month_date, forecast_generation_year, cy_3m_case_equivalent_volume, cy_6m_case_equivalent_volume, cy_12m_case_equivalent_volume, py_3m_case_equivalent_volume, py_6m_case_equivalent_volume, py_12m_case_equivalent_volume, is_current_forecast_generation, forecast_status, updated_at, data_source.
- Forecast init draft chains
  - Columns align to chains final select (includes parent_chain_code and analogous fields).
- Budget generated + primary method
  - As per `sp_seed_zero_for_planned_products_budget.sql` column list and `DEPLETIONS_BUDGET_PRIMARY_METHOD` schema.
- Primary methods (forecast + chains)
  - Forecast PM includes FGMD in PK; chains PM has composite PK without FGMD.

## Key Predicates
- Eligibility: `CORE/APOLLO_DIST_MASTER.IS_DEPLETIONS_ELIGIBLE = 1`
- Exclusions: market-level and distributor-level arrays in `APOLLO_VARIANT_SIZE_PACK_TAG`
- Planned: `IS_PLANNED = TRUE`

## Idempotency
- All inserts guarded by LEFT JOIN NOT EXISTS or MERGE on natural keys (including FGMD/cycle and method).
- Deletes target only keys in PURGE_SET for the current anchors.

## Observability
- Return summary string (counts by table).
- Optionally write per-call metrics to `FORECAST.SYNC_LOG(call_id, table_name, rows_affected, started_at, finished_at, vsp_count)`.

## Risks and Mitigations
- Chain surface availability: derive from existing chains for FGMD; if none exist for a pair, skip chain seeding (can extend later with chain mapping source).
- Schema drift in dbt models: use explicit insert column lists; include smoke tests in CI.

## Security/Permissions
- Procedure needs DML on affected FORECAST and MASTER_DATA tables.
