# Dagster OSS Migration — Design

## Architecture overview

- Code location: Python package `dagster_apollo` at repo root defines a `Definitions` object that loads dbt assets from `tenants/dbt_williamgrant` via `dagster-dbt`.
- dbt integration: Use `DbtProject` and `DbtCliResource` to prepare deps/manifest and execute `dbt build`/`seed`/`snapshot` (R-003, R-004, R-007, R-009).
- Runtime profiles.yml: On process start, a small utility generates `profiles.yml` into a temp directory and sets `DBT_PROFILES_DIR` so dbt reads env-provided credentials; no secrets in Git (R-001, R-002, R-008). By default, do not include `schema` (R-015) [[memory:7867320]].
- Targets: Use `DBT_TARGET` env (dev/prod) or pass `--target` via CLI integration. Dev for local, prod for Render prod (R-005, R-012).
- OSS instance: For Render, configure Dagster instance to use Postgres-backed run storage/event storage so webserver and daemon coordinate (R-011). Local uses default filesystem instance (R-014).
- Deploy: Render Blueprint (`render.yaml`) defines two services: `dagster-webserver` (web) and `dagster-daemon` (worker). Enable automatic deploys on `main` (R-010, R-011).
- CI: GitHub Actions runs `dbt deps && dbt parse` with pinned versions to validate PRs (R-013).

## Diagram

```mermaid
flowchart LR
  subgraph Developer Local [Local Dev]
    DDev[dagster dev\nwebserver+daemon]
    DEnv[.env -> profiles.yml]
  end

  subgraph Render [Render]
    W[dagster-webserver\nweb service]
    Da[dagster-daemon\nworker]
    RDS[(Postgres\nrun+event storage)]
  end

  subgraph Repo [Repo]
    Pkg[dagster_apollo\nDefinitions]
    DBT[tenants/dbt_williamgrant\n(dbt project)]
  end

  SF[(Snowflake)]

  Pkg -->|loads manifest| DBT
  DDev -->|uses| Pkg
  W -->|loads| Pkg
  Da -->|executes runs| Pkg

  DDev -->|materialize| SF
  W --> RDS
  Da --> RDS
  Da -->|dbt CLI| SF

  DEnv --> DDev
```

## Interfaces and data contracts

- Environment variables (read at process start) (R-001, R-002, R-005, R-008, R-015):
  - DBT_PROFILE_NAME: default `apollo-snowflake` (matches `dbt_project.yml`).
  - DBT_TARGET: `dev` (local) | `prod` (Render prod).
  - SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_ROLE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE.
  - SNOWFLAKE_PRIVATE_KEY_PEM or SNOWFLAKE_PRIVATE_KEY_B64 (one required), optional SNOWFLAKE_PRIVATE_KEY_PASSPHRASE.
  - Optional: DBT_THREADS (default 8-24 depending on env), DBT_SCHEMA (discouraged; omit by default per policy [[memory:7867320]]).
  - DAGSTER_HOME (Render): path used by webserver/daemon processes.
  - DAGSTER_PG_URI (Render): Postgres connection string for instance storage (R-011).

- profiles.yml generated structure (Snowflake, dbt-core 1.10) (R-001, R-002, R-009, R-015):
  - profile name = `apollo-snowflake` (from `DBT_PROFILE_NAME`).
  - targets: `dev`, `prod`; each target includes account/user/role/private_key/database/warehouse/threads. Omit `schema` unless `DBT_SCHEMA` explicitly set (R-015).
  - Auth: key pair via `private_key` (PEM string) or `private_key_path` if provided; prefer PEM from env.

- Dagster instance configuration (Render) (R-011):
  - Use `dagster-postgres` storages configured via `DAGSTER_PG_URI` in `dagster.yaml` for runs/event logs.
  - Both `dagster-webserver` and `dagster-daemon` share the same instance config through identical env.

- Asset loading (R-003, R-004):
  - Use `DbtProject.prepare_if_dev()` to ensure `dbt deps` and `dbt parse` run where needed to create `manifest.json`.
  - `@dbt_assets(manifest=my_project.manifest_path)` exposes models/seeds/snapshots as assets; execution uses `DbtCliResource`.

- Scheduling (R-012):
  - Create `build_schedule_from_dbt_selection` with `dbt_select: fqn:*` on `prod` target.

## Key decisions and trade-offs

- Dagster OSS on Render with separate webserver/daemon services and Postgres instance: required for coordination and resilient scheduling (vs. single `dagster dev`, which is dev-only) (R-010, R-011). See Dagster deployment docs (webserver/daemon/instance) for OSS guidance.
- profiles generation in Python (not checked-in Jinja-based profile): avoids secrets in Git and allows strict validation at boot (R-001, R-002, R-006, R-008).
- Omit `schema` in profiles by default, relying on dbt model configs to route to correct schemas; allow optional override via `DBT_SCHEMA` (R-015) [[memory:7867320]].
- Use `DbtProject` + `DbtCliResource` rather than RPC server; aligns with dagster-dbt guidance and supports manifest-based asset graph (R-003).
- Render auto-deploy on main branch; optional gated deploy via Deploy Hook triggered from GitHub Actions after CI success (R-010, R-013).

## Risks and mitigations

- Snowflake auth errors (key format/passphrase): Validate envs at startup; log redacted diagnostics (R-006, R-016).
- Manifest drift or parse failures: CI `dbt deps && dbt parse` blocks merges (R-013).
- Multi-process instance mismatch: Enforce `DAGSTER_PG_URI` and identical `DAGSTER_HOME` in both services (R-011).
- Version compatibility: Pin versions in `requirements.txt`; add a constraints file if needed (R-009).

## References

- Dagster & dbt overview and examples: [Dagster dbt integration](https://docs.dagster.io/integrations/libraries/dbt)
- `dagster-dbt` APIs (`DbtProject`, `DbtCliResource`, `dbt_assets`): [Integration reference](https://docs.dagster.io/api/libraries/dagster-dbt)
- Load dbt models as assets, scaffold project: [Guide](https://docs.dagster.io/integrations/libraries/dbt/creating-a-dbt-project-in-dagster/load-dbt-models)
- Dagster OSS deployment, webserver/daemon, instance config: [Deployment overview](https://docs.dagster.io/deployment), [Webserver](https://docs.dagster.io/guides/operate/webserver), [Instance config](https://docs.dagster.io/guides/deploy/dagster-instance-configuration)
- dbt Core profiles and env vars: [profiles.yml](https://docs.getdbt.com/docs/core/connect-data-platform/profiles.yml), [Environment variables](https://docs.getdbt.com/docs/build/environment-variables)
- dbt Snowflake setup/auth: [Snowflake setup](https://docs.getdbt.com/docs/core/connect-data-platform/snowflake-setup)
