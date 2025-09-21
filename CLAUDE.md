# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Follow these rules at all times @/Users/davidkim/.claude/RULES.md

## Common Development Commands

### dbt Commands
```bash
# William Grant tenant (multi-tenant DBT project)
cd tenants/dbt_williamgrant
dbt build                                    # Build all models
dbt build --select marts.fact              # Build fact models only
dbt build --select model_name              # Build specific model
dbt test                                    # Run data quality tests
dbt docs generate && dbt docs serve        # Generate and serve documentation
dbt clean                                   # Clean target directory
```

### Python Environment
```bash
# Activate conda environment
conda activate apollo
```

## Architecture Overview

### Multi-Tenant dbt Architecture
- **tenants/dbt_williamgrant/**: Primary multi-tenant DBT project for William Grant & Sons
- **apollo_dbt/**: Legacy PostgreSQL project (deprecated, migrated to multi-tenant structure)
- **tenants/**: Directory for additional future tenants (structure mirrors dbt_williamgrant)

### Database Strategy
The project supports both PostgreSQL and Snowflake:
- **PostgreSQL Functions**: Located in `sql_functions/` directory
- **Multi-tenant DBT**: Supports both PostgreSQL and Snowflake adapters
- **Hybrid Tables**: Used for high-performance operations in Snowflake
- **Snowflake SQL Script UDFs and SPs**: Used for complex logic in Snowflake

### Core Data Model Layers
1. **Staging**: Initial data transformations and cleansing
   - Master data: Customer hierarchy, SKU master, GSV rates
   - VIP data: Sales and depletions from three-tier distribution
   - IFS data: Additional core systems integration

2. **Marts**: 
   - **Fact**: Sales fact, depletions summary, account-level sales
   - **Master**: Markets master, account master, distributor master
   - **Forecast**: Forecast models and publishing workflows

### Key Workflows
- **Forecast Editing**: Complete workflow for forecast creation, editing, and versioning
- **Forecast Publishing**: Multi-level publishing with validation and rollback capabilities
- **Product Tagging**: Automated product categorization and tagging system

## Project Structure

### Critical Directories
- `sql_functions/`: PostgreSQL functions for analytics and forecasting
  - `forecast_query_function.sql`: Forecasting capabilities with flexible parameters
  - `forecast_versioning_functions.sql`: Versioning for forecast data
  - `analytical_query_params.sql`: PostgreSQL session optimization

- `tenants/dbt_williamgrant/`: Multi-tenant DBT project
  - `models/`: DBT models (staging, marts)
  - `macros/`: DBT macros for business logic
  - `tests/`: Data quality tests
  - `pg_func_migration/`: Migration artifacts for PostgreSQL functions

- `docs/`: Comprehensive documentation
  - `adr/`: Architecture Decision Records
  - `README_NEW_USERS.md`: Quick start guide
  - `README_REQUIREMENTS.md`: Business requirements
  - `README_FORECAST_VERSIONING_FLOW.md`: Forecast workflow documentation

### Schema Organization
**Multi-tenant DBT (tenants/dbt_williamgrant):**
- `VIP`: Sales and depletions data
- `MASTER_DATA`: Master data tables
- `FORECAST`: Forecast tables and procedures
- `IFS`: Financial systems integration

**Legacy PostgreSQL (apollo_dbt):**
- `wg_vip_s3`: VIP sales and depletions data
- `wg_master_data`: Master data tables
- `wg_forecast`: Forecast-related tables and functions

## Development Patterns

### dbt Model Materialization Strategy
- **Staging**: Views for flexibility and cost efficiency
- **Marts**: Tables for performance in production
- **Materialized Views**: For high-frequency access patterns

### Function Management
- PostgreSQL functions are deployed via `sql_functions/` directory
- Multi-tenant DBT supports both PostgreSQL and Snowflake adapters
- Snowflake stored procedures require manual deployment through migration scripts

### Testing Strategy
- dbt tests for data quality validation
- Custom tests in `tests/` directory for business logic validation
- QA scripts in `qa_scripts/` for data validation

## Environment Configuration

### Required Environment Variables
```bash
# Database Configuration
DB_HOST
DB_PORT
DB_NAME
DB_USER
DB_PASSWORD

# AWS S3 (if using S3 for data storage)
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
S3_BUCKET_NAME

# OpenAI (if using AI features)
OPENAI_API_KEY
```

### dbt Profiles
- `apollo-snowflake`: DBT profile supporting Snowflake for William Grant client

## Multi-Tenant Architecture

This project follows a multi-tenant architecture:
- Each tenant has its own DBT project in the `tenants/` directory
- Shared PostgreSQL functions in `sql_functions/` directory
- Common QA scripts and documentation
- Scalable structure for adding new clients