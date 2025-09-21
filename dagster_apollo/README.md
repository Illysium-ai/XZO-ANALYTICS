# Dagster runbook

## Local development

1) Activate env and install deps

```
conda activate apollo-analytics
pip install -r requirements.txt
```

2) Copy `.env.example` to `.env` and fill Snowflake/dbt vars (key pair recommended)

3) Start the UI

```
DAGSTER_HOME=$(mktemp -d) dagster dev -m dagster_apollo -h 127.0.0.1 -p 3000
```

4) Run dbt
- Assets: select and Materialize selected (one-off)
- Jobs: `dbt_build_job` is configurable. In Launchpad set run config, for example:

```
ops:
  run_dbt_build:
    config:
      exclude: ["seeds/"]
      target: dev
```

## Render deployment

- Use `render.yaml` (webserver + daemon). Enable Auto Deploys on your branch.
- Set env: `DAGSTER_PG_*`, `DBT_PROFILE_NAME=apollo-snowflake`, `DBT_TARGET=prod`, `SNOWFLAKE_*` (key pair), optional `SNOWFLAKE_SCHEMA`.
- The instance config template is at `deploy/dagster/dagster.yaml` (copied to `$DAGSTER_HOME/dagster.yaml` in the container).

## Schedules
- `materialize_dbt_models_prod` (02:00 UTC): manifest-driven full build
- `daily_dbt_build_excluding_seeds` (03:00 UTC): `dbt build` with `exclude: ["seeds/"]`

## Notes
- Profiles are generated at runtime from env; no secrets in Git.
- Schema is not hardcoded; if `DBT_SCHEMA` is absent, `SNOWFLAKE_SCHEMA` is used for dbt validation.
