🤖 Agent: @pm-architect.mdc
💡 Goal: Deploy updated SPs and dbt models to APOLLO_DEVELOPMENT and run zero-coverage + exclusions + purge + budget tests end-to-end.

🔧 Prerequisites
- Activate project env: conda activate apollo-analytics [[memory:6161033]]
- Ensure Snow CLI is authenticated and default connection points to the dev account/role/warehouse.
- All SQL files here include USE DATABASE statements; run with -f (not -q) [[memory:5030023]].

🔧 Step 1: Deploy stored procedures to APOLLO_DEVELOPMENT
- Batch update VSP tags (with is_planned/exclusions/custom support, plus trigger hooks)
```bash
snow sql -f /Users/davidkim/illysium/apollo-analytics/tenants/dbt_williamgrant/backend_functions/sf_product_tagging_workflow/sp_batch_update_apollo_variant_size_pack_tags.sql
```
- Bulk remap IDs across tables
```bash
snow sql -f /Users/davidkim/illysium/apollo-analytics/tenants/dbt_williamgrant/backend_functions/sf_product_tagging_workflow/sp_remap_variant_size_pack_id.sql
```
- Realtime purge + budget regeneration sync (consolidated)
```bash
snow sql -f /Users/davidkim/illysium/apollo-analytics/tenants/dbt_williamgrant/backend_functions/sf_product_tagging_workflow/sp_sync_planned_products_realtime.sql
```
- Budget generation (integrates zero backfill call)
```bash
snow sql -f /Users/davidkim/illysium/apollo-analytics/tenants/dbt_williamgrant/backend_functions/sf_budget_workflow/sp_generate_budget.sql
```

🔧 Step 2: Backfill planning flags and an exclusion for targeted testing
```bash
snow sql -f /Users/davidkim/illysium/apollo-analytics/tenants/dbt_williamgrant/backend_functions/testing_suite/dev_backfill_is_planned.sql
```

🔧 Step 3: Build dbt models for forecast and allocation (DFID is dbt-sourced; SP does not seed DFID)
- Ensure dbt target points to APOLLO_DEVELOPMENT (profiles.yml) and run:
```bash
dbt run --select \
  depletions_forecast_init_draft \
  depletions_forecast_init_draft_chains \
  distributor_allocation_by_market_size_pack \
  distributor_allocation_by_market_size_pack_chains \
  depletions_forecast_primary_forecast_method \
  depletions_forecast_primary_forecast_method_chains
```

🔧 Step 4: Validate forecast zero-seeded coverage (init_draft + chains)
- Expect rows with data_source='zero_seeded' for planned, non-excluded products (12 months × 5 methods)
```sql
SELECT data_source, COUNT(*) AS rows
FROM APOLLO_DEVELOPMENT.FORECAST.DEPLETIONS_FORECAST_INIT_DRAFT
GROUP BY data_source
ORDER BY data_source;
```
- Chains model split:
```sql
SELECT data_source, COUNT(*) AS rows
FROM APOLLO_DEVELOPMENT.FORECAST.DEPLETIONS_FORECAST_INIT_DRAFT_CHAINS
GROUP BY data_source
ORDER BY data_source;
```
- Spot-check a planned product with no history:
```sql
SELECT market_code, distributor_id, variant_size_pack_id, forecast_month_date, forecast_method, data_source
FROM APOLLO_DEVELOPMENT.FORECAST.DEPLETIONS_FORECAST_INIT_DRAFT
WHERE data_source='zero_seeded'
ORDER BY 1,2,3,4,5
LIMIT 200;
```

🔧 Step 5: Validate exclusions honored (examples from backfill script and ad-hoc)
- Confirm market 'USAPA1' is excluded for VSP 'BV002-12-750':
```sql
SELECT COUNT(*) AS cnt
FROM APOLLO_DEVELOPMENT.FORECAST.DEPLETIONS_FORECAST_INIT_DRAFT
WHERE variant_size_pack_id='BV002-12-750'
  AND market_code='USAPA1'
  AND data_source='zero_seeded';
-- Expect cnt = 0
```
- Optional distributor exclusion check (set one via batch update first; replace <DISTRIBUTOR_ID>):
```sql
SELECT COUNT(*) AS cnt
FROM APOLLO_DEVELOPMENT.FORECAST.DEPLETIONS_FORECAST_INIT_DRAFT
WHERE variant_size_pack_id='<VSP_WITH_DISTRIBUTOR_EXCLUSION>'
  AND distributor_id=<DISTRIBUTOR_ID>
  AND data_source='zero_seeded';
-- Expect cnt = 0
```

🔧 Step 6: Validate distributor allocation models include zero-seeded combos
- Base model:
```sql
SELECT COUNT(*) AS rows
FROM APOLLO_DEVELOPMENT.FORECAST.DISTRIBUTOR_ALLOCATION_BY_MARKET_SIZE_PACK;
```
- Allocation consistency (percentages sum to 1 per (market_code, vsp)):
```sql
WITH sums AS (
  SELECT market_code, variant_size_pack_id,
         SUM(distributor_allocation) AS alloc_sum
  FROM APOLLO_DEVELOPMENT.FORECAST.DISTRIBUTOR_ALLOCATION_BY_MARKET_SIZE_PACK
  GROUP BY 1,2
)
SELECT COUNT(*) AS ok
FROM sums
WHERE ABS(alloc_sum - 1.0) < 1e-6;
```
- Chains model (optional similar checks)
```sql
SELECT COUNT(*) AS rows
FROM APOLLO_DEVELOPMENT.FORECAST.DISTRIBUTOR_ALLOCATION_BY_MARKET_SIZE_PACK_CHAINS;
```

🔧 Step 7: Validate is_depletions_eligible = 1 filtering for zero-seeded rows
```sql
SELECT COUNT(*) AS ineligible_zero_seeded
FROM APOLLO_DEVELOPMENT.FORECAST.DEPLETIONS_FORECAST_INIT_DRAFT f
LEFT JOIN APOLLO_DEVELOPMENT.CORE.APOLLO_DIST_MASTER adm
  ON f.distributor_id = adm.distributor_id
WHERE f.data_source = 'zero_seeded'
  AND COALESCE(adm.is_depletions_eligible, 0) != 1;
-- Expect ineligible_zero_seeded = 0
```

🔧 Step 8: Trigger-based behaviors via batch update (hooks)
- 8a. Set a VSP planned TRUE (triggers budget regeneration, including zero backfill)
```sql
CALL APOLLO_DEVELOPMENT.MASTER_DATA.SP_BATCH_UPDATE_APOLLO_VARIANT_SIZE_PACK_TAGS(
  '[{"variant_size_pack_id":"<VSP_FOR_PLANNED_TRUE>","is_planned":true}]'
);
```
Validate budget-generated and primary method for current cycle (autoseeded by hook):
```sql
SELECT COUNT(*) AS rows
FROM APOLLO_DEVELOPMENT.FORECAST.DEPLETIONS_BUDGET_GENERATED
WHERE variant_size_pack_id = '<VSP_FOR_PLANNED_TRUE>'
  AND budget_cycle_date = DATE_TRUNC('month', CURRENT_DATE)::DATE;

SELECT COUNT(*) AS rows
FROM APOLLO_DEVELOPMENT.FORECAST.DEPLETIONS_BUDGET_PRIMARY_METHOD
WHERE variant_size_pack_id = '<VSP_FOR_PLANNED_TRUE>'
  AND budget_cycle_date = DATE_TRUNC('month', CURRENT_DATE)::DATE;
```
- 8b. Set a VSP planned FALSE (triggers purge)
```sql
CALL APOLLO_DEVELOPMENT.MASTER_DATA.SP_BATCH_UPDATE_APOLLO_VARIANT_SIZE_PACK_TAGS(
  '[{"variant_size_pack_id":"<VSP_FOR_PLANNED_FALSE>","is_planned":false}]'
);
```
Validate purge across forecast/budget for the active cycle and FGMD:
```sql
-- Forecast init draft (distributor + chains)
SELECT COUNT(*) AS cnt
FROM APOLLO_DEVELOPMENT.FORECAST.DEPLETIONS_FORECAST_INIT_DRAFT
WHERE variant_size_pack_id = '<VSP_FOR_PLANNED_FALSE>'
  AND forecast_generation_month_date = (SELECT MAX(forecast_generation_month_date) FROM APOLLO_DEVELOPMENT.FORECAST.DEPLETIONS_FORECAST_INIT_DRAFT);

SELECT COUNT(*) AS cnt
FROM APOLLO_DEVELOPMENT.FORECAST.DEPLETIONS_FORECAST_INIT_DRAFT_CHAINS
WHERE variant_size_pack_id = '<VSP_FOR_PLANNED_FALSE>'
  AND forecast_generation_month_date = (SELECT MAX(forecast_generation_month_date) FROM APOLLO_DEVELOPMENT.FORECAST.DEPLETIONS_FORECAST_INIT_DRAFT_CHAINS);

-- Budget generated + primary method (current cycle)
SELECT COUNT(*) AS cnt
FROM APOLLO_DEVELOPMENT.FORECAST.DEPLETIONS_BUDGET_GENERATED
WHERE variant_size_pack_id = '<VSP_FOR_PLANNED_FALSE>'
  AND budget_cycle_date = DATE_TRUNC('month', CURRENT_DATE)::DATE;

SELECT COUNT(*) AS cnt
FROM APOLLO_DEVELOPMENT.FORECAST.DEPLETIONS_BUDGET_PRIMARY_METHOD
WHERE variant_size_pack_id = '<VSP_FOR_PLANNED_FALSE>'
  AND budget_cycle_date = DATE_TRUNC('month', CURRENT_DATE)::DATE;

-- Manual inputs (if any exist in dev)
SELECT COUNT(*) AS cnt
FROM APOLLO_DEVELOPMENT.FORECAST.MANUAL_INPUT_DEPLETIONS_FORECAST
WHERE variant_size_pack_id = '<VSP_FOR_PLANNED_FALSE>'
  AND forecast_generation_month_date = (SELECT MAX(forecast_generation_month_date) FROM APOLLO_DEVELOPMENT.FORECAST.DEPLETIONS_FORECAST_INIT_DRAFT);

SELECT COUNT(*) AS cnt
FROM APOLLO_DEVELOPMENT.FORECAST.MANUAL_INPUT_DEPLETIONS_BUDGET
WHERE variant_size_pack_id = '<VSP_FOR_PLANNED_FALSE>'
  AND budget_cycle_date = DATE_TRUNC('month', CURRENT_DATE)::DATE;
-- Expect cnt = 0 for all queries above
```

🔧 Step 9: Manual budget seeding (idempotent) and validation
- Choose a dev cycle date (e.g., first of current month):
```sql
-- Deprecated: unified realtime sync now seeds planned products automatically
-- CALL APOLLO_DEVELOPMENT.FORECAST.SP_SEED_ZERO_FOR_PLANNED_PRODUCTS_BUDGET(DATE_TRUNC('month', CURRENT_DATE)::DATE, 'dev_tester');
```
- Validate inserts (should include logic_driven and/or zero_seeded data_source):
```sql
SELECT data_source, COUNT(*) AS rows
FROM APOLLO_DEVELOPMENT.FORECAST.DEPLETIONS_BUDGET_GENERATED
WHERE budget_cycle_date = DATE_TRUNC('month', CURRENT_DATE)::DATE
GROUP BY data_source;
```
- Validate budget primary method defaults:
```sql
SELECT COUNT(*) AS rows
FROM APOLLO_DEVELOPMENT.FORECAST.DEPLETIONS_BUDGET_PRIMARY_METHOD
WHERE budget_cycle_date = DATE_TRUNC('month', CURRENT_DATE)::DATE;
```

🔧 Step 10: Full budget generation (logic + zero backfill in one call)
- Generates from published consensus (if present), then backfills zeros + defaults:
```sql
CALL APOLLO_DEVELOPMENT.FORECAST.SP_GENERATE_BUDGET(DATE_TRUNC('month', CURRENT_DATE)::DATE, 'dev_tester');
```
- Re-run validations from Step 9; results should include both logic_driven and zero_seeded records.

🔧 Step 11: DFID recomputation via dbt (SP does not insert DFID); optional null semantics test (no zero-seeding, actuals still eligible)
- Ensure a VSP with actuals has is_planned set to NULL:
```sql
CALL APOLLO_DEVELOPMENT.MASTER_DATA.SP_BATCH_UPDATE_APOLLO_VARIANT_SIZE_PACK_TAGS(
  '[{"variant_size_pack_id":"<VSP_WITH_ACTUALS>","is_planned":null}]'
);
```
- Rebuild forecast init models (required when testing DFID; optional otherwise if daily runs aren’t scheduled in dev):
```bash
dbt run --select depletions_forecast_init_draft depletions_forecast_init_draft_chains
```
- Validate that rows exist for this VSP but none are zero_seeded:
```sql
SELECT data_source, COUNT(*) AS rows
FROM APOLLO_DEVELOPMENT.FORECAST.DEPLETIONS_FORECAST_INIT_DRAFT
WHERE variant_size_pack_id = '<VSP_WITH_ACTUALS>'
GROUP BY data_source;
-- Expect rows for data_source in ('actual_complete','forecast'), none for 'zero_seeded'
```

🔧 Step 12: Custom product scenario — planned TRUE end-to-end
- Create/update a custom product as planned:
```sql
CALL APOLLO_DEVELOPMENT.MASTER_DATA.SP_BATCH_UPDATE_APOLLO_VARIANT_SIZE_PACK_TAGS(
  '[
    {
      "variant_size_pack_id":"CUST_TEST_6x750",
      "variant_size_pack_desc":"Custom Test 6x750",
      "is_custom_product": true,
      "is_planned": true,
      "market_code_exclusions": [],
      "customer_id_exclusions": []
    }
  ]'
);
```
- Re-run dbt Step 3; validate zeros appear in init_draft for this VSP (Step 4 checks) and budget-generated/defaults are populated (Step 9 checks).

🔧 Step 13: Remap custom to system ID (bulk update approach)
- Dry run:
```sql
CALL APOLLO_DEVELOPMENT.MASTER_DATA.SP_REMAP_VARIANT_SIZE_PACK_ID('CUST_TEST_6x750','SYS_TEST_6x750', TRUE, 'dev_tester');
```
- Execute:
```sql
CALL APOLLO_DEVELOPMENT.MASTER_DATA.SP_REMAP_VARIANT_SIZE_PACK_ID('CUST_TEST_6x750','SYS_TEST_6x750', FALSE, 'dev_tester');
```
- Validate presence of only the canonical ID across forecast/budget tables and primary methods.

🔧 Step 14: Cleanup (optional; reset planned flags/exclusions to original; budgets can be regenerated idempotently)
- Revert planned flags and exclusions for test VSPs to NULL and remove ad-hoc exclusions as needed:
```sql
CALL APOLLO_DEVELOPMENT.MASTER_DATA.SP_BATCH_UPDATE_APOLLO_VARIANT_SIZE_PACK_TAGS(
  '[{"variant_size_pack_id":"<VSP_FOR_PLANNED_TRUE>","is_planned":null}]'
);
CALL APOLLO_DEVELOPMENT.MASTER_DATA.SP_BATCH_UPDATE_APOLLO_VARIANT_SIZE_PACK_TAGS(
  '[{"variant_size_pack_id":"<VSP_FOR_PLANNED_FALSE>","is_planned":null}]'
);
```
- Rebuild forecast init models if desired:
```bash
dbt run --select depletions_forecast_init_draft depletions_forecast_init_draft_chains
```

✅ Result
- Zero coverage seeded into forecast (init_draft/_chains) and allocation models for all planned, non-excluded products without history.
- Purge executed automatically on is_planned=false changes via batch SP hooks; re-materialization also enforces exclusion in dbt.
- Budget-generated populated with zero rows for planned combos and default budget primary methods inserted (via hook and manual call).
- Exclusions and distributor eligibility verified end-to-end; optional custom/remap scenarios validated.