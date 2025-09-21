### Architectural Overview

- Hybrid table `MASTER_DATA.APOLLO_VARIANT_SIZE_PACK_TAG` remains the source of truth for product-level planning flags and tags.
- Introduce `is_custom_product` to explicitly distinguish custom entries from system entries.
- Extend batch tagging SP to handle creation/upsert of custom rows and flags/exclusions.
- Add dedicated SPs for:
  - Zero seeding across Forecast and Budget modules
  - ID remapping (bulk update) with audit and conflict resolution
- Optional canonicalization layer via a mapping table for parent/child aliasing.

### Schema Changes

1) `MASTER_DATA.APOLLO_VARIANT_SIZE_PACK_TAG`
- Add columns (Hybrid Table, default values, not null where noted):
  - `IS_PLANNED BOOLEAN NOT NULL DEFAULT TRUE`
  - `MARKET_CODE_EXCLUSIONS ARRAY NOT NULL DEFAULT ARRAY_CONSTRUCT()`
  - `CUSTOMER_ID_EXCLUSIONS ARRAY NOT NULL DEFAULT ARRAY_CONSTRUCT()`
  - `IS_CUSTOM_PRODUCT BOOLEAN NOT NULL DEFAULT FALSE`  <-- new
- Allow INSERT for rows without corresponding `APOLLO_SKU_MASTER` when `IS_CUSTOM_PRODUCT = TRUE` and `VARIANT_SIZE_PACK_DESC` provided.

2) Optional Canonicalization Table
- `MASTER_DATA.VARIANT_SIZE_PACK_CANONICAL_MAP` (Hybrid or Standard):
  - `ALIAS_VARIANT_SIZE_PACK_ID VARCHAR PRIMARY KEY`
  - `CANONICAL_VARIANT_SIZE_PACK_ID VARCHAR NOT NULL`
  - `MAPPED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()`
  - `MAPPED_BY VARCHAR`
  - Unique constraint on `(ALIAS_VARIANT_SIZE_PACK_ID)`; index on `CANONICAL_VARIANT_SIZE_PACK_ID`.

3) Audit Tables
- `MASTER_DATA.CUSTOM_PRODUCT_AUDIT`:
  - `EVENT_ID BIGINT IDENTITY PRIMARY KEY`
  - `EVENT_TYPE VARCHAR` ('create', 'update', 'toggle_planned', 'update_exclusions', 'seed_zero', 'remap')
  - `VARIANT_SIZE_PACK_ID VARCHAR`
  - `PAYLOAD VARIANT`
  - `ROW_COUNT INTEGER`
  - `CORRELATION_ID VARCHAR`
  - `ACTOR VARCHAR`
  - `EVENT_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()`

### Stored Procedures

1) `MASTER_DATA.SP_BATCH_UPDATE_APOLLO_VARIANT_SIZE_PACK_TAGS(P_JSON_STR VARCHAR)`
- Input JSON per item:
  - `variant_size_pack_id` (required)
  - `variant_size_pack_desc` (required when creating custom)
  - `tag_names` (optional)
  - `is_custom_product` (optional Boolean)
  - `is_planned` (optional Boolean)
  - `market_code_exclusions` (optional ARRAY of VARCHAR)
  - `customer_id_exclusions` (optional ARRAY of VARCHAR)
- Behavior:
  - When not found in table:
    - If `is_custom_product = true` or provided flag true, INSERT allowing missing `APOLLO_SKU_MASTER` with given desc.
    - Otherwise, validate presence in SKU master before insert.
  - When found, UPDATE fields that are present in payload (partial updates allowed).
  - Maintain existing tag insert/lookup logic.
  - Write audit row per processed record with counts.

2) `FORECAST.SP_SEED_ZERO_FOR_CUSTOM_PRODUCT(P_VSP_ID VARCHAR, P_FGMD DATE, P_BUDGET_CYCLE_DATE DATE, P_CORR_ID VARCHAR)`
- Resolve markets/distributors universe from reference marts used in budget/forecast generation.
- Respect `MARKET_CODE_EXCLUSIONS`, `CUSTOMER_ID_EXCLUSIONS`, and `IS_PLANNED`.
- Forecast seeding (MANUAL_INPUT_DEPLETIONS_FORECAST):
  - Horizon months derived from `P_FGMD` and config (e.g., next 12/18 months).
  - Methods: 'three_month','six_month','twelve_month','flat','run_rate'.
  - MERGE upsert zero rows (idempotent) keyed by business key+FGMD.
- Budget seeding (MANUAL_INPUT_DEPLETIONS_BUDGET):
  - Months for cycle date year; MERGE zero rows keyed by business key+cycle.
- Audit one event with totals.

3) `MASTER_DATA.SP_REMAP_VARIANT_SIZE_PACK_ID(P_OLD_ID VARCHAR, P_NEW_ID VARCHAR, P_STRATEGY VARCHAR, P_CORR_ID VARCHAR)`
- Preconditions:
  - Validate `P_NEW_ID` exists in `APOLLO_SKU_MASTER` or designated canonical.
  - If canonicalization table is enabled and `P_STRATEGY='canonical'`, insert mapping only and return.
- Bulk Update Strategy (`P_STRATEGY='bulk_update'`):
  - For each impacted table:
    - Attempt UPDATE of `VARIANT_SIZE_PACK_ID` from old to new.
    - Handle unique constraint collisions by merge rules: prefer non-null manual values; sum volumes only when keys differ by ID only and data sources align; otherwise maintain versions by inserting new version rows and removing duplicates.
  - Tables:
    - `FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST`
    - `FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST_VERSIONS`
    - `FORECAST.MANUAL_INPUT_DEPLETIONS_BUDGET`
    - `FORECAST.MANUAL_INPUT_DEPLETIONS_BUDGET_VERSIONS`
    - `FORECAST.DEPLETIONS_FORECAST_PUBLISHED_FORECASTS`
    - `FORECAST.DEPLETIONS_BUDGET_GENERATED` (re-buildable; consider purge/regenerate)
    - `FORECAST.DEPLETIONS_FORECAST_INIT_DRAFT` and `_CHAINS` (dbt models; rebuild)
    - `FORECAST.DEPLETIONS_FORECAST_PRIMARY_FORECAST_METHOD` and `_CHAINS`
  - Wrap in a single transaction; write per-table row counts to audit.

- Post-conditions:
  - Update `MASTER_DATA.APOLLO_VARIANT_SIZE_PACK_TAG` to set `IS_CUSTOM_PRODUCT=false` and align desc to canonical.
  - Optionally keep an alias record (or mapping entry) to maintain traceability.

### API/Frontend Flows

- POST /custom-products
  - Validates auth; calls batch SP with one record; then calls zero-seed SP.
- PATCH /custom-products/{vspId}
  - Partial update for desc, is_planned, exclusions, tags; calls batch SP.
- POST /custom-products/{vspId}/seed-zero
  - Re-run zero seeding for changed exclusions or horizon.
- POST /custom-products/remap
  - Body: { old_id, new_id, strategy: 'bulk_update'|'canonical' }
  - Calls remap SP; returns audit summary.

Frontend
- Creation form with ID+Desc, planned toggle, exclusions pickers.
- Remap tool with validation, dry-run preview (row counts per table), confirmation modal.
- Visual cues for custom vs system products; filters.

### Data Integrity & Audit
- Hybrid tables for instant consistency on user updates.
- All SPs log to `CUSTOM_PRODUCT_AUDIT` with correlation ID.
- Optional dry-run mode for remap SP computing affected row counts without changes.
- Time Travel window ≥ 7 days; backup plan documented.

### Diagrams (ASCII)

Products
[AVSP_TAG] --holds--> [is_custom,is_planned,exclusions,tags]
      |  
      +--(create custom)-> [Zero Seed SP] -> [Forecast/Budget Manual Tables]  
      +--(remap)---------> [Remap SP] -> update all tables -> audit

Canonicalization (optional)
[ALIAS ID] -> [CANONICAL ID] mapping
Reads resolve to canonical in views/UDTFs.

### Risks & Mitigations
- Duplicate rows on remap: Use MERGE with unique keys, define conflict resolution.
- Large universe size: Batch seeding with staging temp tables; set statement timeout limits.
- User error on remap target: Require validation and optional dry-run.
- Exclusion drift: Store exclusions centrally; always reference tag table at read time in UDTFs.
