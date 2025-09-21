# Quick Start Guide for New Users

Welcome to the William Grant Sales & Analytics Platform. This guide will help you quickly understand the project, set up your environment, and start working with the key database objects.

## Project Overview

- The repository is designed for processing and analyzing data for the alcohol beverage industry, specifically tailored for the three-tier distribution system.
- It includes comprehensive SQL view definitions built with dbt, PostgreSQL functions optimized with PostgreSQL 16 enhancements, and a multi-tenant architecture.
- The platform supports multiple clients through the `tenants/` directory structure.

## Environment Setup

1. Ensure you have Python 3.9+ installed and activate the conda environment:

   ```bash
   conda activate apollo
   ```

2. Install the required packages:

   ```bash
   pip install -r requirements.txt
   pip install dbt-postgres dbt-snowflake
   ```

3. Configure your `.env` file with the appropriate database and AWS credentials.

4. Set up your dbt profile in `~/.dbt/profiles.yml`:

   ```yaml
   apollo-snowflake:
     target: dev
     outputs:
       dev:
         type: postgres  # or snowflake for production
         host: "{{ env_var('DB_HOST') }}"
         port: "{{ env_var('DB_PORT') | as_number }}"
         dbname: "{{ env_var('DB_NAME') }}"
         user: "{{ env_var('DB_USER') }}"
         pass: "{{ env_var('DB_PASSWORD') }}"
         schema: wg_vip_s3
         threads: 4
   ```

## Key Components

### 1. Multi-Tenant DBT Projects

The SQL view definitions are built and managed via dbt in the `tenants/` directory. Each tenant has its own dbt project with dedicated models, macros, and configurations.

To build all models using dbt, run:

```bash
cd tenants/dbt_williamgrant
dbt deps
dbt build                # Build all models using dbt
```

To build specific models (e.g., staging and fact models), run:

```bash
cd tenants/dbt_williamgrant
dbt build --select staging marts.fact    # Build specific model types
```

### 2. PostgreSQL Functions

- Stored in the `sql_functions/` directory.
- Example: `forecast_query_function` provides forecasting capabilities and trend analysis.
- `analytical_query_params` optimizes PostgreSQL settings for analytical workloads.

### 3. Quality Assurance Scripts

The `qa_scripts/` directory contains Python scripts for data validation and testing:

- **`check_complete_months.py`**: Validates data completeness by month
- **`run_qa_query.py`**: Executes quality assurance queries
- **`run_debug_query.py`**: Debugging and troubleshooting scripts

## Database Access and Example Queries

All database objects are maintained in the `wg_vip_s3` schema. To connect and query, use:

```bash
PGPASSWORD=$DB_PASSWORD psql \
 -h $DB_HOST \
 -p $DB_PORT \
 -d $DB_NAME \
 -U $DB_USER \
 -c "SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = 'wg_vip_s3' AND table_name IN ('vw_dim_outlet_v3', 'vw_dim_product_v3', 'vw_dim_distributor_v3', 'vw_sales_fact_v3') ORDER BY table_name, ordinal_position;"
```

Example SQL query to view aggregated sales data:

```sql
SELECT 
  year,
  month,
  state,
  brand,
  variant,
  SUM(total_case_equivalent_sales) AS total_case_eq,
  SUM(total_sales_dollars) AS total_dollars
FROM 
  wg_vip_s3.vw_market_variant_size_summary_v3
WHERE 
  month = 'February 2025'
GROUP BY 
  year, month, state, brand, variant;
```

## Getting Started with DBT

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

## Additional Documentation

For detailed usage and technical information, refer to:

- `README_REQUIREMENTS.md`: Business and technical requirements for the project.
- `README_FORECAST_VERSIONING_FLOW.md`: Forecast versioning workflow documentation.
- `adr/`: Architecture Decision Records for key design decisions.

## Best Practices

- Always test changes in a development environment before production deployment.
- Adhere to coding and SQL styling guidelines for consistency and performance.
- Use explicit joins and clear aggregations in your queries.
- Cross-reference outputs from multiple views to ensure data consistency.
- Use the `set_analytical_query_params()` function before running complex analytical queries.
- Follow the multi-tenant architecture for adding new clients.

## Troubleshooting

If you encounter errors:

- Verify that your conda environment is activated: `conda activate apollo`
- Run `dbt debug` to verify database connectivity and profile configuration
- Check dbt logs for detailed error messages (`logs/dbt.log` in the project directory)
- Verify your database credentials in the `.env` file
- Ensure that your database is accessible and all required schemas exist

## Contact and Support

For further assistance or to report issues, please use the project's issue tracker or reach out to the development team. 