- Activate env and verify Snow CLI [[memory:5930361]].
```bash
conda activate apollo-analytics
snow sql -q "USE DATABASE APOLLO_DEVELOPMENT; USE SCHEMA FORECAST; SELECT CURRENT_DATABASE(), CURRENT_SCHEMA();"
```

- Create dev tables (run once) [[memory:5030023]].
```bash
snow sql -f tenants/dbt_williamgrant/backend_functions/sf_budget_workflow/snowflake_budget_tables_ddl.sql
```

- Create a dev view over generated+manual (so UDTF reads primary/overrides without dbt).
```sql
snow sql -q "
USE DATABASE APOLLO_DEVELOPMENT; USE SCHEMA FORECAST;
CREATE OR REPLACE VIEW FORECAST.VW_DEPLETIONS_BUDGET_FINAL AS
WITH base AS (
  SELECT * FROM FORECAST.DEPLETIONS_BUDGET_GENERATED
),
manual AS (
  SELECT * FROM FORECAST.MANUAL_INPUT_DEPLETIONS_BUDGET
)
SELECT
  b.MARKET_NAME, b.MARKET_CODE, b.DISTRIBUTOR_NAME, b.DISTRIBUTOR_ID,
  b.BRAND, b.BRAND_ID, b.VARIANT, b.VARIANT_ID, b.VARIANT_SIZE_PACK_DESC, b.VARIANT_SIZE_PACK_ID,
  b.FORECAST_YEAR, b.MONTH, b.FORECAST_MONTH_DATE, b.FORECAST_METHOD,
  COALESCE(m.MANUAL_CASE_EQUIVALENT_VOLUME, b.CASE_EQUIVALENT_VOLUME) AS CASE_EQUIVALENT_VOLUME,
  b.PY_CASE_EQUIVALENT_VOLUME,
  b.DATA_SOURCE,
  b.BUDGET_CYCLE_DATE,
  (m.MANUAL_CASE_EQUIVALENT_VOLUME IS NOT NULL) AS IS_MANUAL_INPUT,
  m.UPDATED_AT,
  m.COMMENT
FROM base b
LEFT JOIN manual m
  ON m.MARKET_CODE = b.MARKET_CODE
 AND m.DISTRIBUTOR_ID = b.DISTRIBUTOR_ID
 AND m.VARIANT_SIZE_PACK_ID = b.VARIANT_SIZE_PACK_ID
 AND m.FORECAST_YEAR = b.FORECAST_YEAR
 AND m.MONTH = b.MONTH
 AND m.BUDGET_CYCLE_DATE = b.BUDGET_CYCLE_DATE;
"
```

- Create UDF/UDTF and SPs (dev only).
```bash
snow sql -f tenants/dbt_williamgrant/backend_functions/sf_budget_workflow/udf_is_budget_approved.sql
snow sql -f tenants/dbt_williamgrant/backend_functions/sf_budget_workflow/udtf_get_depletions_budget.sql
snow sql -f tenants/dbt_williamgrant/backend_functions/sf_budget_workflow/sp_generate_budget.sql
snow sql -f tenants/dbt_williamgrant/backend_functions/sf_budget_workflow/sp_batch_save_budgets.sql
snow sql -f tenants/dbt_williamgrant/backend_functions/sf_budget_workflow/sp_approve_budget.sql
```

- Generate Y+1 for a chosen FGMD (budget cycle).
```bash
snow sql -q "USE DATABASE APOLLO_DEVELOPMENT; USE SCHEMA FORECAST; CALL SP_GENERATE_BUDGET('2025-08-01','dev_user');"
```

- Primary method seeding is now automatic: `SP_GENERATE_BUDGET` derives and inserts `DEPLETIONS_BUDGET_PRIMARY_METHOD` from the published consensus methods in `DEPLETIONS_FORECAST_PUBLISHED_FORECASTS` for the same FGMD. Existing rows are not overwritten (idempotent). Fallback is `six_month` when no published method exists. No manual seeding step is required.


- Read budgets (primary-only default).
```sql
snow sql -q "
USE DATABASE APOLLO_DEVELOPMENT; USE SCHEMA FORECAST;
SELECT *
FROM TABLE(
  FORECAST.UDTF_GET_DEPLETIONS_BUDGET(
    P_BUDGET_CYCLE_DATE => '2025-07-01'::DATE,
    P_FORECAST_METHOD => NULL::VARCHAR,
    P_MARKETS => [],
    P_CUSTOMERS => [],
    P_VARIANT_SIZE_PACK_IDS => [],
    P_ONLY_PRIMARY => TRUE
  )
)
LIMIT 100;
"
```

- Batch edit volumes and/or update budget primary method (method-agnostic volume; optional selected_forecast_method).
```sql
snow sql -q "
USE DATABASE APOLLO_DEVELOPMENT; USE SCHEMA FORECAST;
CALL SP_BATCH_SAVE_BUDGETS($$
[
  {
    \"market_code\": \"USANY1\",
    \"customer_id\": \"12345\",
    \"variant_size_pack_id\": \"VSP-001\",
    \"forecast_year\": 2026,
    \"month\": 1,
    \"manual_case_equivalent_volume\": 123.45,
    \"selected_forecast_method\": \"six_month\",
    \"comment\": \"QA edit\"
  }
]
$$, '2025-08-01', 'dev_user');
"
```

- Approve/lock cycle; verify saves are blocked.
```sql
snow sql -q "USE DATABASE APOLLO_DEVELOPMENT; USE SCHEMA FORECAST; CALL SP_APPROVE_BUDGET('2025-08-01','approver','ready to lock');"

snow sql -q "
USE DATABASE APOLLO_DEVELOPMENT; USE SCHEMA FORECAST;
CALL SP_BATCH_SAVE_BUDGETS($$[{\"market_code\":\"USANY1\",\"customer_id\":\"12345\",\"variant_size_pack_id\":\"VSP-001\",\"forecast_year\":2026,\"month\":1,\"manual_case_equivalent_volume\": 200.0}]$$,'2025-08-01','dev_user');
"  # expect error due to lock
```

- Optional: Read all methods for a row (confirm manual override appears across methods).
```sql
snow sql -q "
USE DATABASE APOLLO_DEVELOPMENT; USE SCHEMA FORECAST;
SELECT * FROM TABLE(FORECAST.UDTF_GET_DEPLETIONS_BUDGET('2025-08-01', NULL, ARRAY_CONSTRUCT('USANY1'), ARRAY_CONSTRUCT('12345'), ARRAY_CONSTRUCT('VSP-001'), FALSE))
WHERE forecast_year = 2026 AND month = 1;
"
```

- Notes
  - All commands target APOLLO_DEVELOPMENT; do not run in production while developing.