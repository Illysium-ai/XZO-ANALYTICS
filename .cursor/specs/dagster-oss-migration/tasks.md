# Dagster OSS Migration — Task Plan

Legend: status = todo | in-progress | completed | blocked

- T-001 — Initialize Dagster project scaffolding and structure (status: completed)
  - Notes: `dagster_apollo/` created, `requirements.txt` pinned, minimal defs export working.

- T-002 — Implement runtime `profiles.yml` generator (status: completed)
  - Notes: `dagster_apollo/profiles.py` generates from env, loads .env, schema fallback to `SNOWFLAKE_SCHEMA`.

- T-003 — Wire `dagster-dbt` assets and CLI resource (status: completed)
  - Notes: `DbtProject` + `DbtCliResource` wired; manifest prepared via `dbt deps/parse`; assets visible locally; schedule added.

- T-004 — Add default production schedule and job (status: completed)
  - Notes: Added `materialize_dbt_models_prod` (02:00 UTC) and `daily_dbt_build_excluding_seeds` (03:00 UTC).

- T-005 — Dagster OSS instance config for Render (Postgres) (status: completed)
  - Notes: Template at `deploy/dagster/dagster.yaml` with env-driven Postgres storages.

- T-006 — Render deployment (Blueprint) (status: completed)
  - Notes: `render.yaml` added; enable Auto Deploys in Render; set env per file.

- T-007 — CI for dbt compile/parse (status: completed)
  - Notes: `.github/workflows/ci.yml` runs `dbt deps` and `dbt parse` on PRs.

- T-008 — Auto-deploy on merge (status: todo)
  - Notes: Enable Render Auto-deploys or add Deploy Hook GH Action.

- T-009 — Local DX and docs (status: completed)
  - Notes: Added `dagster_apollo/README.md` with local/Render runbooks; `.env.example` updated previously.

- T-010 — Migrate/replicate dbt Cloud schedules (status: todo)

- T-011 — Governance: steering updates (status: completed)
  - Notes: Appended Dagster/dagster-dbt/deployment notes to `.cursor/steering/tech.md` and `structure.md`.

- T-012 — Decommission dbt Cloud (status: todo)

- T-013 — PR template and CHANGELOG hygiene (status: todo)
