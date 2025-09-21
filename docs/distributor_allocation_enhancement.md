# Distributor Allocation Gap Filling Enhancement

## Overview
Enhanced the `tenants/dbt_williamgrant/models/marts/forecast/distributor_allocation_by_market_size_pack.sql` model to include gap-filling functionality that identifies forecast combinations missing from the allocation model and evenly distributes them across all corresponding distributors.

### Enhancement Date
Current session

### Problem Statement
The original allocation model only included combinations that had historical sales data in the last 12 months. However, the forecast model may contain additional market + variant_size_pack combinations that should have distributor allocations for completeness. These gaps needed to be filled with even distribution across all active distributors in the corresponding markets.

### Solution Architecture
Implemented a layered gap processing architecture that:
1. Preserves all existing allocation logic completely
2. Identifies missing combinations through efficient gap detection
3. Maps gaps to all active distributors in corresponding markets
4. Calculates even distribution across distributors
5. Combines original and distributed allocations seamlessly

### Technical Implementation

#### New CTEs Added

1. **`allocation_gaps`**
   - Combined logic: Identifies forecast combinations missing from allocation in single CTE
   - Queries `depletions_forecast_init_draft` with NOT EXISTS clause for efficiency
   - Filters for `data_type = 'forecast'` and `is_current_forecast_generation = 1`
   - Directly identifies gaps without intermediate step

2. **`distributor_universe_by_market`**
   - Gets all distributors for each market from forecast model (not sales data)
   - Sources from `depletions_forecast_init_draft` to capture all distributor combinations
   - Ensures coverage of distributors that may only appear in forecast data
   - Critical for manual interventions and new distributor combinations

3. **`distributed_allocations`**
   - Creates equal percentage allocation records for gaps
   - Uses `1.0 / count(distributor_id)` for equal distribution (e.g., 25% each for 4 distributors)
   - Maintains same schema structure as original allocation data

4. **`combined_allocations`**
   - Unions original allocation data with distributed records
   - Preserves data integrity and schema consistency
   - Provides comprehensive coverage of all combinations

#### Key Features

- **Optimized CTE Structure**: Combined gap identification and detection into single efficient CTE
- **Correct Data Source**: Distributor universe sourced from forecast model to capture all combinations including manual interventions
- **Equal Percentage Distribution**: Fair allocation based on distributor count per market (e.g., 25% each for 4 distributors)
- **Volume Independence**: Distribution based on percentage allocation, not actual forecast volumes
- **Complete Coverage**: Ensures all distributors that appear in forecast model are included
- **Data Lineage**: Clear separation between calculated and distributed allocations
- **Performance Optimization**: Simplified processing without unnecessary intermediate steps
- **Schema Consistency**: Maintains existing model structure and output format

#### Design Optimizations

**CTE Consolidation**: 
- Original design had separate `forecast_combinations` and `allocation_gaps` CTEs
- Optimized to single `allocation_gaps` CTE with combined logic for better performance

**Data Source Correction**:
- Original approach sourced distributors from sales data (`rad_distributor_level_sales`)
- Optimized to source from forecast model (`depletions_forecast_init_draft`) to capture:
  - Manual intervention distributors
  - New distributor combinations
  - Future SKU distributors that may not have sales history

#### Changes to Final Output

- Updated final select to use `combined_allocations` instead of `deps_by_market_distro`
- Preserved existing allocation percentage calculation logic
- Maintained incremental materialization strategy and unique_key

### Usage

The enhanced model now provides:
- All existing allocation calculations (unchanged functionality)
- Complete coverage of forecast combinations through gap-filling
- Even distribution for combinations without historical sales data
- Consistent allocation percentages that sum to 1.0 for each combination

### Benefits

1. **Complete Coverage**: All forecast combinations now have distributor allocations
2. **Fair Distribution**: Equal representation when historical data is unavailable
3. **Backward Compatibility**: No breaking changes to existing functionality
4. **Data Integrity**: Volume conservation and allocation accuracy maintained
5. **Performance Efficiency**: Targeted processing of only missing combinations
6. **Future Flexibility**: Architecture supports additional distribution methods

### Business Logic

**Allocation Sources:**
- **Historical Data**: Uses actual sales data for allocation percentages (original logic)
- **Gap Filling**: Uses equal percentage distribution for forecast combinations without sales history

**Distribution Algorithm:**
- Identifies forecast combinations missing from allocation data
- Counts active distributors in the corresponding market
- Distributes equally: `1.0 / distributor_count` (e.g., 0.25 each for 4 distributors)
- Creates allocation records that result in equal percentage distribution

**Example:**
- Market has 4 active distributors for a gap combination
- Each distributor gets `1.0 / 4 = 0.25` as their `sum_case_equivalent_depletions`
- Total becomes `4 × 0.25 = 1.0` for the combination
- Final allocation percentages: `0.25 / 1.0 = 25%` for each distributor

### Testing Recommendations

1. Verify record counts include both historical and gap-filled allocations
2. Confirm allocation percentages sum to 1.0 for each market + variant combination
3. Validate even distribution accuracy for gap-filled records
4. Test incremental runs to ensure proper data refresh
5. Verify volume conservation between forecast and allocation totals
6. Confirm no duplicate records exist for the same combination

### Performance Considerations

- Gap detection uses efficient NOT EXISTS clause
- Processing limited to missing combinations only
- Leverages existing indexes on market_code and variant_size_pack_id
- Maintains existing incremental strategy performance
- Window functions used for efficient distributor counting

### Maintenance Notes

- Monitor performance with additional forecast combinations
- Ensure `depletions_forecast_init_draft` model remains accessible
- Consider adjusting time window if business requirements change
- Update distribution logic if alternative allocation methods are needed 