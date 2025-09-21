### Technology Stack

- **Warehouse**: Snowflake (SQL, VARIANT/ARRAY semi-structured types)
- **Transformations**: dbt projects per tenant under `tenants/`
- **Procedural logic**: Snowflake SQL stored procedures/UDF/UDTF in `backend_functions`
- **Execution**: Snow CLI (`snow sql`); use `-f` when scripts include `USE` statements; prefer parameterized cursors and bind variables
- **Data modeling**: `models/`, `macros/`, `seeds/`, and `tests/` per tenant; macros for shared SQL patterns
- **Standards**:
  - Set-based operations over RBAR; use `ARRAY_CONSTRUCT`, `FLATTEN`, and CTEs for clarity
  - Bind variables (`:var`) inside SQL blocks; `BEGIN TRANSACTION`/`COMMIT` with all-or-nothing semantics for batch ops
  - Clear distinction between omitted vs empty inputs for partial updates
  - Consistent casing/normalization for tag names (`INITCAP`, `TRIM`)
- **Error handling**: Raise specific exceptions with actionable messages; rollback on errors to maintain integrity
- **Performance**: Minimize unnecessary scans; use EXISTS/joins for upserts; avoid deep procedural loops
- **Security**: Do not commit secrets; respect Snowflake RBAC; avoid dynamic SQL unless required

- **Feature notes — Product tagging SPs**:
  - Presence of `tag_names` controls updates; empty arrays are allowed and clear fields; non-array values are ignored for tag updates while other fields can proceed.

- **Orchestration (Dagster OSS)**:
  - Use `dagster==1.11.8`, `dagster-dbt==0.27.8`, `dbt-core==1.10.10`, `dbt-snowflake==1.10.0`.
  - dbt assets loaded via manifest using `DbtProject` + `DbtCliResource`.
  - Profiles are generated at runtime from env; `DBT_SCHEMA` not hardcoded by default; fallback to `SNOWFLAKE_SCHEMA` for dbt validation only.
  - Instance storage in Render backed by Postgres (see `deploy/dagster/dagster.yaml`).
  - Webserver and daemon run as separate services (see `render.yaml`).
  - Prefer a single parameterized `dbt_build_job` over multiple fixed jobs; pass select/exclude/vars/target via run config.
