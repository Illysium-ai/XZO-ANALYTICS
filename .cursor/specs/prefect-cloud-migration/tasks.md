# Prefect Cloud Migration for dbt Core — Tasks

Status legend: todo | in-progress | blocked | completed

- T-101 Repo setup (flows skeleton) — links: R-101, R-202 — status: completed
  - Outcome: `flows/dbt_wg_flow.py` entrypoint created with parameters per design.

- T-102 Dependencies pinning — links: R-109 — status: completed
  - Outcome: `requirements-prefect.txt` added with `prefect`, `prefect-dbt[snowflake]`, and `dbt-snowflake` pinned to 1.7.x; deployment can reference this file.

- T-103 Secrets & Blocks — links: R-110, R-201 — status: in-progress
  - Outcome: Script `scripts/prefect/setup_dbt_blocks.py` added; next, run it with Snowflake env vars to create the `DbtCliProfile` block in Prefect Cloud.

- T-104 Local validation (dev) — links: R-203, R-204 — status: todo
  - Outcome: Using local env (`conda activate apollo-analytics`), run flow locally against `dev` target; validate selectors/vars; compare node set to dbt Cloud baseline.

- T-105 Deploy to Prefect Cloud — links: R-102, R-108, R-112 — status: todo
  - Outcome: `uvx prefect-cloud deploy flows/dbt_wg_flow.py:run_dbt_wg --from <org>/<repo> --name dbt_wg --with-python 3.12 --with prefect --with "prefect-dbt[all_extras]" --with dbt-snowflake==<pin> --parameter command=build --parameter target=dev` succeeds.

- T-106 Create schedules — links: R-103, R-113 — status: todo
  - Outcome: Two schedules created with per‑schedule params: one for `dev`, one for `prod` (cron matching dbt Cloud cadence); verified in Prefect UI.

- T-107 Ad‑hoc run & overrides — links: R-104 — status: todo
  - Outcome: `uvx prefect-cloud run run_dbt_wg/dbt_wg --parameter target=prod --parameter vars='{"key":"value"}' --follow` works; logs tail successfully.

- T-108 Parity validation — links: R-116 — status: todo
  - Outcome: Documented comparison of node counts/status vs last dbt Cloud run; explain any differences.

- T-109 Cutover — links: R-114 — status: todo
  - Outcome: Disable dbt Cloud schedules; keep as rollback for one cycle; monitor Prefect runs.

- T-110 Runbook & docs — links: R-115 — status: todo
  - Outcome: Add operator runbook covering ad‑hoc runs, schedule edits, secret rotation, and debugging patterns.

- T-111 Post‑cutover monitoring — links: R-204 — status: todo
  - Outcome: First week success criteria met; alerts/log patterns reviewed; finalize rollback decommission.

## Notes

- Use Prefect Quickstart commands for deploy/run/schedule. See: https://docs.prefect.io/v3/get-started/github-quickstart
- Integration details and dbt Core examples: https://docs.prefect.io/integrations/prefect-dbt and https://docs.prefect.io/v3/examples/run-dbt-with-prefect
