### Overview

Enable clients to add and manage “custom products” (variant size pack id/desc) not present in the source dataset, and ensure seamless participation in budgeting and forecasting with robust remapping to canonical system IDs when available.

### Scope
- Table: `MASTER_DATA.APOLLO_VARIANT_SIZE_PACK_TAG`
- Procedures: tagging batch updater, zero-seeding (budget/generated + primary methods), ID remapping
- Models/tables impacted by zero seeding: `FORECAST.DEPLETIONS_FORECAST_INIT_DRAFT` and `_CHAINS` (dbt), `FORECAST.DISTRIBUTOR_ALLOCATION_BY_MARKET_SIZE_PACK` and `_CHAINS` (dbt), `FORECAST.DEPLETIONS_BUDGET_GENERATED` (SP), `FORECAST.DEPLETIONS_BUDGET_PRIMARY_METHOD` (SP), `FORECAST.DEPLETIONS_FORECAST_PRIMARY_FORECAST_METHOD` and `_CHAINS` (dbt)
- Zero seeding applies to all products with `is_planned = true` (system and custom), not only custom products

### Definitions
- Custom Product: A product row with `is_custom_product = true` in `MASTER_DATA.APOLLO_VARIANT_SIZE_PACK_TAG` that may not exist in `MASTER_DATA.APOLLO_SKU_MASTER`.
- Canonical/System Product: Product present in `MASTER_DATA.APOLLO_SKU_MASTER` or otherwise designated as the authoritative product ID.
- Exclusions: `market_code_exclusions` and `customer_id_exclusions` arrays indicating where the product should not appear.

### Requirements (EARS)

- R-001 (Data model – is_custom_product): The system shall add a Boolean column `is_custom_product` to `MASTER_DATA.APOLLO_VARIANT_SIZE_PACK_TAG` to explicitly flag custom products.  
  Acceptance: Column exists; default `false`; non-nullable with default; backfilled for existing rows.

- R-002 (Data model – planned): The system shall persist `is_planned` (Boolean, default true) per `variant_size_pack_id` to control planning eligibility.  
  Acceptance: Column exists with default true; toggles reflected in downstream fetch UDTFs within 1 minute (or immediately for Hybrid Table reads).

- R-003 (Data model – exclusions): The system shall persist `market_code_exclusions` and `customer_id_exclusions` arrays per `variant_size_pack_id`.  
  Acceptance: Columns exist with empty array default; queries can filter out excluded combinations efficiently.

- R-004 (Creation without SKU master dependency): The system shall allow insertion of custom products into `APOLLO_VARIANT_SIZE_PACK_TAG` even when the ID is not present in `APOLLO_SKU_MASTER`, provided `is_custom_product = true` and a `variant_size_pack_desc` is supplied.  
  Acceptance: Attempting to create a custom product with missing SKU master row succeeds and stores provided description.

- R-005 (Default availability): The system shall make planned products (`is_planned = true`) available by default for budgeting and forecasting across all markets and distributors, except where excluded by the two exclusion arrays or explicitly unplanned.  
  Acceptance: Planned products appear in forecasting/budget across all markets/distributors except excluded ones.

- R-006 (Batch update SP): The system shall extend the batch SP `MASTER_DATA.SP_BATCH_UPDATE_APOLLO_VARIANT_SIZE_PACK_TAGS` to accept and upsert:
  - `is_custom_product`
  - `is_planned`
  - `market_code_exclusions`
  - `customer_id_exclusions`
  - `variant_size_pack_desc` (required when creating custom products)
  Acceptance: JSON payloads with the above fields upsert correctly; validation errors are descriptive.

- R-007 (Zero seeding – Forecast init draft): The system shall include zero forecast rows in `FORECAST.DEPLETIONS_FORECAST_INIT_DRAFT` and `_CHAINS` for all planned products and all five methods {`three_month`, `six_month`, `twelve_month`, `flat`, `run_rate`} across the next 12 months, for all non-excluded market/distributor (and chain) combinations that have no historical depletions to generate forecasts.  
  Acceptance: dbt models union zero rows for planned, non-excluded combinations where `combined_data` would otherwise be empty; idempotent across runs.

- R-008 (Distributor allocation zero coverage): The system shall ensure `FORECAST.DISTRIBUTOR_ALLOCATION_BY_MARKET_SIZE_PACK` and `_CHAINS` include allocations for planned products even when forecast combinations would otherwise be missing, by gap-filling using even distribution across active distributors/chains in each market.  
  Acceptance: Models output allocation rows for planned product combinations; percentages are evenly distributed and sum to 1.0.

- R-009 (Zero seeding – Budget generated): The system shall insert zero records into `FORECAST.DEPLETIONS_BUDGET_GENERATED` for planned products, for the active budget cycle date, across all months of the budget year, for all non-excluded market/distributor combinations not present in the generated result set.  
  Acceptance: Procedure populates missing rows with `CASE_EQUIVALENT_VOLUME = 0` and a reasonable default method; idempotent.

- R-010 (Primary methods defaults): The system shall insert default primary methods for planned products into `FORECAST.DEPLETIONS_BUDGET_PRIMARY_METHOD` (during budget generation) and ensure `FORECAST.DEPLETIONS_FORECAST_PRIMARY_FORECAST_METHOD` and `_CHAINS` (dbt) include planned zero-seeded combinations with a default method when missing.  
  Acceptance: Default method is set to `six_month` when none is available from configuration/history; no duplicates.

- R-011 (Remapping capability): The system shall provide a remapping mechanism to replace any custom product ID with a canonical system product ID when available, moving and/or merging all relevant data.  
  Acceptance: After remap, only the system ID appears in outputs; all user inputs and histories are preserved per merge rules.

- R-012 (Remapping – bulk update option): The system shall support a transactional bulk update procedure that updates `variant_size_pack_id` across all relevant tables, with configurable merge strategy on duplicates.  
  Acceptance: Procedure completes atomically, logs row counts per table, and leaves no orphaned or duplicate rows.

- R-013 (No double counting): The system shall ensure that, post-remapping and when actuals start for the canonical ID, there is no double counting in forecasting/budgeting outputs.  
  Acceptance: Regression tests confirm no duplication when both IDs had prior data.

- R-014 (Auditability): The system shall record all custom-product creates/updates, zero-seeding operations, and remapping actions with who/when/what and row counts per table.  
  Acceptance: Audit tables are populated; entries are queryable for reconciliation.

- R-015 (Security & RBAC): Only authorized roles can create/remap products or change planning flags/exclusions.  
  Acceptance: Role checks enforced in SPs; attempts are denied with clear errors.

- R-016 (Performance): Zero-seeding at scale (5k distributor-market combinations, 60 months horizon) and remapping should complete within target SLAs (3–5 minutes) under realistic conditions.  
  Acceptance: Load tests within thresholds.

- R-017 (Idempotency): Seeding and remapping operations shall be idempotent and safe to retry without side-effects.  
  Acceptance: Re-running operations does not create duplicates or change totals beyond intended merge rules.

### Non-Functional Requirements
- Observability: Structured logs and audit tables with correlation IDs.
- Backups: Time Travel windows configured on impacted tables ≥ 7 days.
- Testing: Unit and integration tests including remap conflict and zero-coverage scenarios.
- Docs: Operator playbooks for creation, toggling, exclusions, and remapping.
