### Project Structure

- **Top-level**:
  - `docs/` — ADRs, data model references, design docs
  - `tenants/` — per-tenant dbt projects and Snowflake functions
  - `.cursor/specs/` — feature specs (requirements/design/tasks)
  - `.cursor/steering/` — foundational steering (this file set)

- **Tenants layout** (`apollo-analytics/tenants/<tenant>`):
  - `dbt_<tenant>/backend_functions/` — Snowflake SPs and DDLs
    - `sf_forecast_editing_workflow/`
    - `sf_forecast_publishing_workflow/`
    - `sf_budget_workflow/`
    - `sf_product_tagging_workflow/`
  - `dbt_<tenant>/models/`, `macros/`, `seeds/`, `tests/`
  - `dbt_<tenant>/dbt_project.yml`, `packages.yml`

- **Database objects and schemas**:
  - Databases: `APOLLO_DEVELOPMENT`, `APOLLO_<TENANT>` (e.g., `APOLLO_WILLIAMGRANT`)
  - Schemas: `MASTER_DATA`, `FORECAST`, others as needed per workflow

- **Conventions**:
  - Descriptive variable names; avoid collapsing semantics (e.g., treat omitted vs empty distinctly)
  - Normalize text inputs; keep SQL readable with CTEs and clear CASE expressions
  - Keep edits minimal and isolated to relevant workflows

- **Feature mapping — Product tagging**:
  - Tables: `MASTER_DATA.APOLLO_PRODUCT_TAGS`, `MASTER_DATA.APOLLO_VARIANT_SIZE_PACK_TAG`
  - Procedures: `MASTER_DATA.SP_BATCH_UPDATE_APOLLO_VARIANT_SIZE_PACK_TAGS`, `MASTER_DATA.SP_ADD_PRODUCT_TAGS_TO_VARIANT_SIZE_PACK`

- **Dagster orchestration code**:
  - `dagster_apollo/` — Python package exporting `Definitions`;
    - `dbt_assets.py` — loads dbt assets, defines `dbt_build_job` (configurable), schedules
    - `profiles.py` — runtime `profiles.yml` generator from env
    - `README.md` — local and Render runbook (append-only)

- **Deployment & CI**:
  - `deploy/dagster/dagster.yaml` — instance config template (Postgres storages)
  - `render.yaml` — Render blueprint (webserver, daemon)
  - `.github/workflows/ci.yml` — PR/main dbt deps + parse
