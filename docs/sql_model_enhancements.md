# SQL Model Enhancements Documentation

## Depletions Forecast Init Draft Model Enhancement

### Overview
Enhanced the `tenants/dbt_williamgrant/models/marts/forecast/depletions_forecast_init_draft.sql` model to include previous consensus forecast records alongside logic-driven forecasts.

### Enhancement Date
Current session

### Problem Statement
The original model only included logic-driven forecasts based on trend analysis. However, previous consensus forecasts may contain records that are not present in the logic-driven calculations but should be included in the comprehensive forecast view.

### Solution Architecture
Implemented a hybrid layered architecture that:
1. Preserves all existing logic-driven forecast functionality
2. Adds previous consensus forecasts as a separate data source
3. Combines both sources through a union with deduplication
4. Maintains clear data lineage

### Technical Implementation

#### New CTEs Added

1. **`previous_consensus_forecasts`**
   - Joins `wg_forecast.depletions_forecast_published_forecasts` with `wg_forecast.depletions_forecast_publications`
   - Filters for records with `publication_status = 'consensus'`
   - Joins on `forecast_generation_month_date - 1 month` condition
   - **Generates forecast_month_date**: Uses `make_date(forecast_year, month, 1)` from integer columns
   - **Critical Filter**: Only includes forecasted months (`forecast_month_date > latest_complete_month_date`) to avoid overwriting actual data with forecasted values
   - Assigns 'run_rate' forecast method to all previous consensus records
   - Sets 'draft' forecast_status as required

2. **`integrated_data`**
   - Unions logic-driven forecasts (from existing `combined_data`)
   - Unions previous consensus forecasts with deduplication logic
   - Adds `data_source` field for clear lineage ('logic_driven' vs 'previous_consensus')

#### Key Features

- **Forecasted Months Only**: Previous consensus records are filtered to only include future forecast periods, preserving actual data integrity for completed months
- **Deduplication Logic**: Prevents duplicate records by excluding previous consensus records that already exist in logic-driven data
- **Data Lineage**: Clear tracking through `data_source` field
- **Schema Alignment**: Ensures all fields are properly mapped between data sources
- **Performance Optimization**: Uses EXISTS clause for efficient deduplication

#### Changes to Incremental Strategy

- Updated incremental WHERE clause to work with `integrated_data` instead of `combined_data`
- Maintains existing incremental performance characteristics
- Handles both data sources in the incremental logic

### Usage

The enhanced model now includes:
- All existing logic-driven forecasts (unchanged functionality)
- Previous consensus forecasts from the prior month with 'run_rate' forecast method
- Clear data source identification for analysis and debugging

### Benefits

1. **Comprehensive Coverage**: Includes all relevant forecast data from both logic-driven calculations and previous consensus decisions
2. **Backward Compatibility**: No breaking changes to existing functionality
3. **Clear Data Lineage**: Easy to identify source of each record
4. **Performance Maintained**: Efficient implementation with minimal performance impact
5. **Future Flexibility**: Architecture supports additional data sources if needed

### Testing Recommendations

1. Verify record counts include both logic-driven and previous consensus forecasts
2. Confirm no duplicate records exist for the same combination of key fields
3. Validate that 'run_rate' forecast method is correctly assigned to previous consensus records
4. Test incremental runs to ensure proper data refresh
5. Verify business logic accuracy for integrated results

### Maintenance Notes

- Monitor performance with the additional data source
- Ensure `wg_forecast.depletions_forecast_published_forecasts` table remains accessible
- Update documentation if additional forecast methods are needed for consensus records 