# Prefect Cloud Migration for dbt Core — Design

This document describes the architecture and operational design to migrate orchestration from dbt Cloud to Prefect Cloud while running dbt Core for the project at `tenants/dbt_williamgrant/`.

## Overview

- Orchestrator: Prefect Cloud deployment created via the GitHub quickstart CLI.
- Workload: dbt Core commands executed through `prefect-dbt` with Snowflake connectivity.
- Topology: A single deployment with parameterized environment (`target`) and per‑schedule parameter defaults for `dev` and `prod`.
- Security: Snowflake credentials stored as Prefect Secrets/Blocks; no plaintext secrets in repo.
- Observability: Prefect Cloud states, logs, and dbt artifacts (where supported) for troubleshooting.

References:
- GitHub Quickstart: https://docs.prefect.io/v3/get-started/github-quickstart
- Integration (dbt Core): https://docs.prefect.io/integrations/prefect-dbt#dbt-core
- Snowflake extras: https://docs.prefect.io/integrations/prefect-dbt#additional-capabilities-for-dbt-core-and-snowflake-profiles
- Example: https://docs.prefect.io/v3/examples/run-dbt-with-prefect
- Example script: https://github.com/PrefectHQ/prefect/blob/main/examples/run_dbt_with_prefect.py

## Architecture Diagram (ASCII)

```
Developer        Prefect Cloud               Runtime Env                 Data Warehouse
----------       -------------               -----------                 --------------
 git push  --->  GitHub-connected deploy  -> Python 3.12 + deps  --->    Snowflake (dbt Core)
                  (single deployment)       prefect-dbt + dbt-snowflake   (schemas via dbt configs)
                                        ^
                                        | logs/states/artifacts
                                        +-- Prefect UI/CLI
```

## Components

- Prefect Cloud Deployment and Schedules
  - One deployment (e.g., `dbt_wg`) with parameters: `command`, `target`, `select`, `exclude`, `vars`, `full_refresh`.
  - Multiple schedules per deployment with per‑schedule parameter defaults, e.g., `target=dev` and `target=prod`.
- Flow Entrypoint (Repository)
  - A small Python module (e.g., `flows/dbt_wg_flow.py`) defining a Prefect flow function that invokes dbt via `prefect-dbt`.
  - Resides alongside the existing dbt project without altering dbt folder layout.
- Credentials & Profiles
  - Prefect Secrets/Blocks to hold Snowflake credentials.
  - `prefect-dbt` blocks (`DbtCliProfile` with `SnowflakeTargetConfigs`) to provide an ephemeral dbt CLI profile at runtime; avoid `~/.dbt` coupling.
- Dependencies & Runtime
  - Python 3.12 runtime (or as validated), `prefect`, `prefect-dbt`, and `dbt-snowflake` pinned to match the project.

## Data & Control Flow

1. Prefect triggers the deployment (scheduled or ad‑hoc), passing parameters (e.g., `target=prod`, `vars=...`).
2. The flow loads required Secrets/Blocks (Snowflake credentials; optional `DbtCliProfile`).
3. The flow constructs and runs the dbt command via `DbtCoreOperation` (e.g., `build`, `run`, `test`) with flags (`--select`, `--exclude`, `--full-refresh`, `--vars`).
4. dbt Core executes against Snowflake; logs and artifacts are surfaced to Prefect where supported.
5. Prefect records state transitions; operators can follow logs in CLI/Cloud UI.

## Contracts

- Entrypoint path (example): `flows/dbt_wg_flow.py:run_dbt_wg`
- Parameters (defaults may be set at deploy/schedule):
  - `command`: string; one of {`build`, `run`, `test`, `seed`, `deps`, `clean`} (default: `build`).
  - `target`: string; environment target in dbt (default: `dev`).
  - `select`: string; optional selector expression.
  - `exclude`: string; optional exclude expression.
  - `vars`: string; YAML/JSON-serializable string passed to `--vars`.
  - `full_refresh`: bool; default: `false`.
  - `state_path`: string; optional path to prior state directory for stateful selection.
  - `project_dir`: string; default: `tenants/dbt_williamgrant/`.
  - `profiles_strategy`: string; default: `blocks` (prefer blocks over local `~/.dbt`).
- Deployment name: `dbt_wg` (single deployment as requested), with multiple schedules.
- Python version: `3.12` (supported by Prefect Cloud quickstart `--with-python`).
- Dependencies: `prefect`, `prefect-dbt`, `prefect-dbt[snowflake]`, and `dbt-snowflake==<pin>`.

## Secrets and Profiles

- Store Snowflake connection details in Prefect Secrets/Blocks (e.g., account, user, auth method, database, warehouse, role). Prefer key‑pair auth when available.
- Use `DbtCliProfile` + `SnowflakeTargetConfigs` to materialize an ephemeral `profiles.yml` at runtime, with `overwrite_profiles=True` scoped to an ephemeral `profiles_dir`.
- Do not hardcode `schema` in code; continue to govern schema via dbt model configs.

## Deployment & Scheduling

- Deploy via GitHub quickstart:
  - `uvx prefect-cloud deploy flows/dbt_wg_flow.py:run_dbt_wg --from <org>/<repo> --name dbt_wg --with-python 3.12 --with prefect --with "prefect-dbt[all_extras]" --with dbt-snowflake==<pin>`
  - Add default parameters with `--parameter` flags (e.g., `--parameter command=build --parameter target=dev`).
- Run on demand:
  - `uvx prefect-cloud run run_dbt_wg/dbt_wg --parameter target=prod --parameter vars='{...}' --follow`
- Schedule with per‑schedule parameters:
  - `uvx prefect-cloud schedule run_dbt_wg/dbt_wg "0 0 * * *" --parameter target=dev`
  - `uvx prefect-cloud schedule run_dbt_wg/dbt_wg "0 6 * * *" --parameter target=prod`

(See Quickstart for exact CLI usage and options.)

## Observability & Failure Handling

- Prefect Cloud UI to inspect states, logs, and artifacts from `prefect-dbt`.
- Utilize retries at the flow/operation level where appropriate.
- For failures, operators can tail logs with `--follow` and consult dbt artifacts.

## Risks & Mitigations

- Profiles overwrite risk: Scope `overwrite_profiles=True` to an ephemeral directory; prefer blocks over `~/.dbt`.
- Secrets sprawl: Centralize all credentials in Prefect Secrets/Blocks.
- Dependency drift: Pin `dbt-snowflake` to the version used in the project; validate locally first.
- Parity drift: Validate selectors and node counts against a baseline dbt Cloud run before cutover.

## Alternatives Considered

- Separate deployments for `dev` and `prod`: clearer blast radius and RBAC separation, but more management overhead. The chosen design keeps a single deployment with per‑schedule parameters as requested for simplicity.
- Shelling out to `dbt` directly: simpler but loses integration benefits; `prefect-dbt` provides better observability and block integration.

## Traceability

- R-101, R-111, R-202: Minimize code changes; avoid schema in orchestration.
- R-102, R-103, R-104, R-108, R-112, R-113: Single deployment, parameterization, schedules via quickstart.
- R-105, R-106, R-107: Use `prefect-dbt` for commands, flags, and artifacts.
- R-109, R-201, R-204: Pinned runtime, secure secrets, observable runs.
- R-114, R-115, R-116: Rollback, runbook, and parity validation.
