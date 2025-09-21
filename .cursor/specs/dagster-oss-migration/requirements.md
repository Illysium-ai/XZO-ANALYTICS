# Dagster OSS Migration — Requirements (EARS)

- R-001 — WHEN Dagster runs locally (developer machine), THE SYSTEM SHALL generate a dbt `profiles.yml` at runtime from environment variables with no secrets in Git.
  - Acceptance: Starting `dagster dev` after exporting env vars generates a temporary `profiles.yml`, `DBT_PROFILES_DIR` is set for the process, dbt connects successfully.

- R-002 — WHEN Dagster runs in Render (production), THE SYSTEM SHALL generate `profiles.yml` at runtime from environment variables managed in Render.
  - Acceptance: Render logs confirm profile generation on boot; no secrets are present in the repo.

- R-003 — WHEN the Dagster code location loads, THE SYSTEM SHALL discover and load dbt assets from `tenants/dbt_williamgrant` using `dagster-dbt` and the dbt manifest.
  - Acceptance: Dagster UI shows assets for all dbt models/seeds/snapshots; node counts match `manifest.json`.

- R-004 — WHEN new dbt models are added/changed, THE SYSTEM SHALL reflect changes in Dagster assets after prepare/parse without manual code edits.
  - Acceptance: Running prepare/parse locally or in CI updates assets; UI reflects new/changed assets.

- R-005 — WHEN executing dbt via Dagster, THE SYSTEM SHALL run with target `dev` or `prod` based on environment (e.g., `DBT_TARGET` for local/Render).
  - Acceptance: Dev runs materialize into development environment; prod runs materialize into production environment.

- R-006 — WHEN required env vars are missing or invalid, THE SYSTEM SHALL fail fast with a clear error pointing to the missing variables.
  - Acceptance: Process exits before attempting dbt; error lists missing keys.

- R-007 — WHEN dbt commands run, THE SYSTEM SHALL capture and surface dbt CLI logs in Dagster event logs for observability.
  - Acceptance: Dagster run page shows streamed dbt logs and error lines.

- R-008 — Security: THE SYSTEM SHALL avoid committing secrets; `.env.example` documents required variables; `.env` is ignored by Git; Render stores secrets.
  - Acceptance: Secret scan passes; no secret values present in repo.

- R-009 — Compatibility: THE SYSTEM SHALL pin versions Dagster 1.11.8, dagster-dbt 0.27.8, dbt-core 1.10.10, dbt-snowflake 1.10.0.
  - Acceptance: Dependency lock/constraints enforce these versions; runtime `pip show`/`pip freeze` matches.

- R-010 — Deployment: THE SYSTEM SHALL be deployable to Render with automatic deploys on merges to the main branch.
  - Acceptance: Merging to main triggers Render deploy; service becomes healthy.

- R-011 — Operations: THE SYSTEM SHALL run Dagster webserver and daemon in Render with a shared Dagster instance backend suitable for multi-process coordination.
  - Acceptance: Webserver and daemon coordinate (schedules, run queue) using a common backend (e.g., Postgres); runs appear in UI and execute.

- R-012 — Schedules: THE SYSTEM SHALL support at least one production schedule mirroring the current dbt Cloud cadence (daily build of all models), plus ad-hoc manual runs.
  - Acceptance: A Dagster schedule exists and triggers materializations; manual build works from UI.

- R-013 — CI: THE SYSTEM SHALL verify dbt project compiles in CI (`dbt deps` + `dbt parse`) on PRs.
  - Acceptance: CI fails on parse errors; success required before merge.

- R-014 — Local DX: THE SYSTEM SHALL support local dev using the conda env `apollo-analytics` and `dagster dev` with documented env vars and quickstart.
  - Acceptance: Following README steps (`conda activate apollo-analytics` then `dagster dev`) loads assets and can materialize dev target. [[memory:6161033]]

- R-015 — Profiles policy: THE SYSTEM SHALL not hardcode Snowflake `schema` in the generated profile by default; schema comes from dbt model configs.
  - Acceptance: Generated `profiles.yml` omits `schema` unless explicitly provided via env var; dbt builds use model-level `+schema` config. [[memory:7867320]]

- R-016 — Observability & errors: THE SYSTEM SHALL expose health endpoints/logs suitable for Render and provide actionable failure messages for Snowflake auth issues.
  - Acceptance: Healthcheck responds (webserver up); auth errors include account/user/role context without leaking secrets.

Traceability: All design elements and tasks must reference these R-### IDs.
