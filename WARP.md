# WARP.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.

- Common commands
- Environment and profiles
- High-level architecture
- Safety and assistant-behavior rules
- Important documentation references
- Scope notes

## Common commands

- Navigate to active tenant dbt project
  - cd tenants/dbt_williamgrant
- Activate Python env (if using conda)
  - conda activate apollo
- Install dbt packages
  - dbt deps
- Build models
  - dbt build
  - dbt build --select staging
  - dbt build --select marts.fact
  - dbt build --select model_name
- Run tests
  - dbt test
  - dbt test -s <model_or_test_selector>
- Generate and view docs
  - dbt docs generate && dbt docs serve
- Clean build artifacts
  - dbt clean
- Seed reference data
  - dbt seed
- Snowflake CLI (values come from README.md env vars)
  - snowsql -a $SNOWFLAKE_ACCOUNT -u $SNOWFLAKE_USER -r $SNOWFLAKE_ROLE -d $SNOWFLAKE_DATABASE -w $SNOWFLAKE_WAREHOUSE

## Environment and profiles

- dbt profile name for this tenant: apollo-snowflake (see tenants/dbt_williamgrant/dbt_project.yml)
- Configure ~/.dbt/profiles.yml using the Snowflake env vars shown in README.md
- .env.example exists but primarily contains Postgres/AWS/OpenAI placeholders; Snowflake setup is documented in README.md

## High-level architecture

- Multi-tenant dbt layout under tenants/, with the active tenant at tenants/dbt_williamgrant
- Data model layers and schemas (see tenants/dbt_williamgrant/dbt_project.yml)
  - staging: materialized as views
  - marts: materialized as tables
    - fact → schema VIP
    - forecast → schema FORECAST
    - master → schema MASTER_DATA
  - seeds are enabled and default to schema MASTER_DATA
- Macros encode core business logic: standardized_size, case_equivalent_factor, case_equivalent_type, days_of_supply; plus forecast utilities (see tenants/dbt_williamgrant/macros/)
- Snowflake backend functions and workflows (tenants/dbt_williamgrant/backend_functions/)
  - Forecast editing, publishing, product tagging, budget workflows via SQL scripts (stored procedures, UDFs/UDTFs). These complement dbt models and operate in Snowflake
- Forecast versioning and publishing (see docs/README_FORECAST_VERSIONING_FLOW.md)
  - Version creation, history, revert, and batch save; allocation from market-level to distributor-level using lookup; published/approved versions feed downstream FORECAST models

## Safety and assistant-behavior rules

- Snow CLI development safety (.cursor/rules/snow-cli-dev-safety.mdc)
  - Default SQL executions to APOLLO_DEVELOPMENT
  - Do not run DDL in production during development
  - Use explicit USE DATABASE/SCHEMA statements when issuing Snowflake SQL
- Keep commands and paths tenant-aware and dbt-focused (see CLAUDE.md for common dbt workflows)

## Important documentation references

- README.md — project overview, setup, Snowflake env vars, quick commands
- docs/README_NEW_USERS.md — quick start for new users
- docs/README_REQUIREMENTS.md — core data requirements and source mappings
- docs/README_FORECAST_VERSIONING_FLOW.md — detailed forecast versioning workflow
- tenants/dbt_williamgrant/README.md — tenant dbt project overview

## Scope notes

- This file is tailored to the current repository contents; avoid inventing commands or paths
- Prefer dbt operations from tenants/dbt_williamgrant; reference Snowflake usage via README.md
- Keep outputs concise and oriented to terminal workflows
