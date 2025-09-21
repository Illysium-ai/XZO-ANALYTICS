# XZO Analytics Platform

This repository contains the XZO analytics platform - a comprehensive data analytics solution for enterprise SAAS deployment. Originally forked from the Apollo analytics platform, it provides tools for building data models using dbt, analyzing sales and inventory data, and supporting multi-tenant data operations for the alcohol beverage industry.

## 🚀 XZO SAAS Features

### Multi-Tenancy
- **Tenant Isolation**: Complete data separation between customers
- **Scalable Architecture**: Built for enterprise deployment
- **Configuration Management**: Tenant-specific settings and customizations

### Enterprise Analytics
- **Real-time Dashboards**: Live analytics and reporting
- **Data Pipeline Orchestration**: Dagster-based workflow management
- **Performance Monitoring**: System health and usage tracking

### SAAS Integration
- **API-First Design**: RESTful APIs for frontend integration
- **Subscription Management**: Usage tracking and billing support
- **Customer Onboarding**: Automated tenant provisioning

## Core Components

### 1. Multi-Tenant DBT Projects

The platform's data models are built using dbt (data build tool) and organized in a multi-tenant architecture within the `tenants/` directory. Each tenant has its own dbt project with dedicated models, macros, and configurations.

#### Current Tenants:
- **XZO Multi-Tenant** (`tenants/dbt_williamgrant/`): SAAS-ready alcohol industry data model (forked from William Grant & Sons)

#### Project Structure:
```
tenants/dbt_williamgrant/
├── models/                # dbt models directory
│   ├── staging/           # Initial data transformations
│   └── marts/             # Final dimensional and fact models
│       ├── fact/          # Fact models
│       ├── master/        # Master data models
│       └── forecast/      # Forecast models
├── macros/                # dbt macros
├── tests/                 # Data quality tests
├── pg_func_migration/     # Snowflake functions migrated from PostgreSQL
│   ├── sf_forecast_editing_workflow/     # Forecast editing stored procedures
│   ├── sf_forecast_publishing_workflow/  # Forecast publishing stored procedures
│   └── sf_product_tagging_workflow/      # Product tagging stored procedures
└── dbt_project.yml        # dbt project configuration
```

#### Data Model Components:
- **Staging Models**: Clean and standardize source data
- **Dimension Models**: Product, outlet, distributor, and time dimensions
- **Fact Models**: Sales facts, depletion facts, summary depletions base
- **Forecast Models**: Forecasting and trend analysis capabilities

#### Building the Models:
```bash
cd tenants/dbt_williamgrant
dbt build                # Build all models using dbt
dbt build --select marts.fact  # Build all fact models
dbt build --select model_name  # Build a specific model
dbt test               # Run tests on models
dbt docs generate && dbt docs serve  # Generate and view documentation
```

### 2. Snowflake Functions

The platform includes several Snowflake stored procedures and user-defined functions (UDFs) that have been migrated from PostgreSQL to enhance analytics capabilities:

#### Forecast Editing Workflow:
- **`sp_batch_save_forecasts.sql`**: Batch save forecasts using optimized MERGE statements
- **`sp_get_depletions_forecast.sql`**: Retrieve depletions forecast data with staged filtering
- **`sp_save_forecast_version.sql`**: Save forecast versions with transaction handling
- **`sp_revert_forecast_to_version.sql`**: Revert forecasts to previous versions
- **`udf_get_valid_forecast_generation_month_date.sql`**: Get valid forecast generation month date

#### Forecast Publishing Workflow:
- **`sp_publish_division_forecast.sql`**: Publish division forecasts with consolidated logic
- **`sp_unpublish_division_forecast.sql`**: Unpublish division forecasts
- **`sp_unpublish_group.sql`**: Unpublish group forecasts
- **`sp_unpublish_market_forecast.sql`**: Unpublish market forecasts
- **`udf_is_depletions_forecast_published.sql`**: Check if depletions forecast is published

#### Product Tagging Workflow:
- **`sp_add_product_tags_to_variant_size_pack.sql`**: Add product tags to variant size packs
- **`sp_batch_update_apollo_variant_size_pack_tags.sql`**: Batch update product tags

#### Using Snowflake Functions:
```sql
-- Execute a stored procedure
CALL APOLLO_WILLIAMGRANT.FORECAST.sp_get_depletions_forecast(
    p_division_id => 'DIV001',
    p_forecast_month => '2024-01-01'
);

-- Use a user-defined function
SELECT APOLLO_WILLIAMGRANT.FORECAST.udf_get_valid_forecast_generation_month_date();
```

### 3. Quality Assurance Scripts

The `qa_scripts/` directory contains Python scripts for data quality validation and testing:

- **`check_complete_months.py`**: Validates data completeness by month
- **`run_qa_query.py`**: Executes quality assurance queries
- **`run_debug_query.py`**: Debugging and troubleshooting scripts
- **`run_combined_debug.py`**: Comprehensive debugging utilities

### 4. Documentation

Comprehensive documentation is provided to help understand the data model and how to use it effectively.

- **`README.md`**: Main project overview and documentation entry point
- **`docs/README_NEW_USERS.md`**: Quick start guide for new users
- **`docs/README_REQUIREMENTS.md`**: Functional requirements for the volume analysis features
- **`docs/README_FORECAST_VERSIONING_FLOW.md`**: Forecast versioning workflow documentation
- **`docs/adr/`**: Architecture Decision Records for key design decisions

## Prerequisites

- Python 3.9+ with conda environment named 'apollo'
- Snowflake data warehouse
- dbt (data build tool) with Snowflake adapter
- AWS credentials (if using S3 for data storage)

## Setup

1. Set up the conda environment:
   ```bash
   conda create -n apollo python=3.9
   conda activate apollo
   ```

2. Install the required Python packages:
   ```bash
   pip install -r requirements.txt
   pip install dbt-snowflake
   ```

3. Configure your dbt profile in `~/.dbt/profiles.yml`:
   ```yaml
   apollo-snowflake:
     target: dev
     outputs:
       dev:
         type: snowflake
         account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
         user: "{{ env_var('SNOWFLAKE_USER') }}"
         password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
         role: "{{ env_var('SNOWFLAKE_ROLE') }}"
         database: "{{ env_var('SNOWFLAKE_DATABASE') }}"
         warehouse: "{{ env_var('SNOWFLAKE_WAREHOUSE') }}"
         schema: "{{ env_var('SNOWFLAKE_SCHEMA') }}"
         threads: 4
   ```

4. Set up environment variables in your `.env` file:
   ```bash
   # Snowflake Configuration
   SNOWFLAKE_ACCOUNT=your_snowflake_account
   SNOWFLAKE_USER=your_snowflake_username
   SNOWFLAKE_PASSWORD=your_snowflake_password
   SNOWFLAKE_ROLE=BACKEND_ENG_ROLE
   SNOWFLAKE_DATABASE=APOLLO_WILLIAMGRANT
   SNOWFLAKE_WAREHOUSE=DATAOPS_WH
   SNOWFLAKE_SCHEMA=VIP

   # AWS Configuration (if using S3)
   AWS_ACCESS_KEY_ID=your_access_key
   AWS_SECRET_ACCESS_KEY=your_secret_key
   S3_BUCKET_NAME=your_bucket_name
   ```

## Getting Started

1. **Navigate to the tenant project**:
   ```bash
   cd tenants/dbt_williamgrant
   ```

2. **Install dependencies**:
   ```bash
   dbt deps
   ```

3. **Build the data models**:
   ```bash
   dbt build
   ```

4. **Run tests**:
   ```bash
   dbt test
   ```

5. **Generate documentation**:
   ```bash
   dbt docs generate && dbt docs serve
   ```

## Database Access

Connect to Snowflake using the configured credentials:

```bash
# Using SnowSQL CLI
snowsql -a $SNOWFLAKE_ACCOUNT -u $SNOWFLAKE_USER -r $SNOWFLAKE_ROLE -d $SNOWFLAKE_DATABASE -w $SNOWFLAKE_WAREHOUSE
```

### Key Database Objects

- **Dimension Views**: Product, outlet, distributor, and time dimensions
- **Fact Views**: Sales facts, depletion facts, summary depletions base
- **Snowflake Functions**: Forecasting, publishing, and product tagging stored procedures

## Troubleshooting

If you encounter errors:

- Verify that your conda environment is activated: `conda activate apollo`
- Run `dbt debug` to verify Snowflake connectivity and profile configuration
- Check dbt logs for detailed error messages (`logs/dbt.log` in the project directory)
- Verify your Snowflake credentials in the `.env` file
- Ensure that your Snowflake warehouse is running and accessible
- Check that you have the appropriate role permissions (BACKEND_ENG_ROLE or ANALYST_ROLE)

## Best Practices

- Test queries with realistic data sets in a development environment before using them in production
- Use explicit joins and aggregation to ensure clarity and performance
- Cross-reference outputs using multiple views for consistency
- Leverage Snowflake's warehouse scaling for complex analytical queries
- Use the migrated stored procedures for forecast operations
- Leverage dbt documentation to understand model relationships and dependencies
- Follow the multi-tenant architecture for adding new clients

## Architecture Decision Records

Key architectural decisions are documented in `docs/adr/`:

- **ADR-001**: Forecast publishing refactor
- **ADR-002**: DBT source configuration
- **ADR-003**: Forecast publishing synchronous sync
- **ADR-004**: Forecast publishing valid FGMD update
- **ADR-005**: Forecast publishing logic
- **ADR-006**: PostgreSQL to Snowflake migration mapping

## Contributing

When contributing to this project:

1. Follow the existing code structure and naming conventions
2. Add appropriate tests for new models and functions
3. Update documentation for any changes
4. Create new ADRs for significant architectural decisions
5. Use the multi-tenant structure for client-specific implementations 