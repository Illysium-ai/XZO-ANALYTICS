# Prefect Cloud Migration for dbt Core — Requirements (EARS)

This document captures requirements for migrating the existing dbt Cloud deployment to Prefect Cloud orchestrating dbt Core for the repository at `tenants/dbt_williamgrant/`.

## Functional Requirements

- R-101: The system shall orchestrate dbt Core for the project rooted at `tenants/dbt_williamgrant/` without modifying schemas outside dbt model configs.
- R-102: The system shall provide a single Prefect deployment with parameters enabling environment selection via `--target` (e.g., `dev`, `prod`).
- R-103: The system shall support per-schedule parameter defaults so that separate schedules can target different environments from the same deployment.
- R-104: The system shall support ad‑hoc runs with parameter overrides (e.g., `--parameter target=dev --parameter vars=...`).
- R-105: The system shall run dbt via `prefect-dbt` for richer state, logs, and artifacts.
- R-106: The system shall accept common dbt CLI options including `command` (e.g., `build`, `run`, `test`), `--select`, `--exclude`, `--vars`, `--full-refresh`, and `--state` when applicable.
- R-107: The system shall surface run status, state transitions, and logs in Prefect Cloud, and make dbt artifacts available for troubleshooting when supported.
- R-108: The system shall allow setting default parameter values at deployment time (e.g., default `command=build`, default `target=dev`).
- R-109: The system shall use a pinned Python runtime version supported by Prefect Cloud (e.g., 3.12) and install required dependencies for the flow.
- R-110: The system shall store secrets and credentials in Prefect Secret/Block constructs (not plaintext in the repo), including Snowflake credentials.
- R-111: The system shall avoid hardcoding Snowflake schema values in orchestration code; schema behavior remains governed by dbt model configs.
- R-112: The system shall be deployable and schedulable using the Prefect Cloud GitHub quickstart CLI (`uvx prefect-cloud ...`).
- R-113: The system shall provide separate schedules (cron) that replicate the current dbt Cloud cadence for dev and prod, respectively.
- R-114: The system shall provide a rollback path (re-enable dbt Cloud schedule or prior deployment version) in case of failures.
- R-115: The system shall include a concise runbook for triggering ad‑hoc runs, modifying schedules, rotating secrets, and debugging failures.
- R-116: The system shall validate parity by comparing node selection and success between dbt Cloud’s last successful run and Prefect’s first successful run(s).

## Non-Functional Requirements

- R-201: Secrets management shall follow the principle of least privilege and avoid plaintext in version control.
- R-202: The design shall minimize changes to existing dbt code and directory structure.
- R-203: The solution shall be reproducible locally (developer workstation) and in Prefect Cloud.
- R-204: The solution shall be observable, emitting actionable logs and clear failure reasons.
- R-205: The solution shall avoid introducing tooling lock-in beyond Prefect and dbt.

## Out of Scope

- R-301: Rewriting dbt models or macros.
- R-302: Changing warehouse sizing, roles, or Snowflake RBAC beyond what’s needed for connectivity.
- R-303: Altering dbt schemas outside of model configurations.

## Acceptance Criteria

- A-101: A Prefect Cloud deployment exists and can successfully run `dbt build` against the `dev` target for `tenants/dbt_williamgrant/` with logs viewable in Prefect Cloud.
- A-102: Two schedules (or more) exist under the single deployment, each with per‑schedule parameter defaults for `target` (`dev` and `prod`), replicating current cron cadence.
- A-103: An operator can trigger an ad‑hoc run with parameter overrides for `target`, `vars`, and `select` without code changes.
- A-104: Snowflake secrets are stored in Prefect secret/block storage and not in the repository; rotating a secret does not require code changes.
- A-105: Python runtime and dependencies are pinned and documented.
- A-106: Parity validation demonstrates that the Prefect‑orchestrated run builds the same set of nodes (or an explained superset/subset) as the last successful dbt Cloud run.
- A-107: A rollback step is documented and verified (e.g., disabling schedules and re‑enabling dbt Cloud or restoring a prior deployment version).

## References

- Prefect Cloud GitHub Quickstart (deploy/run/schedule): https://docs.prefect.io/v3/get-started/github-quickstart
- prefect-dbt integration docs (dbt Core, blocks, logging): https://docs.prefect.io/integrations/prefect-dbt
- dbt Core section: https://docs.prefect.io/integrations/prefect-dbt#dbt-core
- Snowflake extras for prefect-dbt: https://docs.prefect.io/integrations/prefect-dbt#additional-capabilities-for-dbt-core-and-snowflake-profiles
- Example: Run dbt with Prefect: https://docs.prefect.io/v3/examples/run-dbt-with-prefect
- Example script: https://github.com/PrefectHQ/prefect/blob/main/examples/run_dbt_with_prefect.py
