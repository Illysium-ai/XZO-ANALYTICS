# Forecast Versioning System Documentation

## Overview

The Forecast Versioning System is a robust PostgreSQL-based solution for managing depletions forecasts with complete version history tracking. This system allows:

- Creating and updating forecast data with automatic versioning
- Tracking all changes made to forecasts over time
- Reverting to previous versions of forecasts when needed
- Handling batch updates through a standardized JSON interface
- Allocating market-level forecasts to individual distributors based on predefined allocation rules

## Database Schema

### Core Tables

#### 1. `wg_forecast.manual_input_depletions_forecast`

Primary table containing the current state of all forecasts.

**Key Fields:**
- `id` - Primary key
- Dimensional fields (market, distributor, brand, etc.)
- `forecast_year` and `month` - Time period being forecasted
- `forecast_method` - Method used for forecasting (e.g., 'flat', 'three_month')
- `manual_case_equivalent_volume` - Forecast value (in case equivalents)
- `updated_by_user_id` - User who last modified the forecast
- `current_version` - Current version number of this forecast
- `forecast_status` - Status of the forecast (e.g., 'saved', 'published')

#### 2. `wg_forecast.manual_input_depletions_forecast_versions`

Append-only history table that tracks all versions of each forecast.

**Key Fields:**
- `version_id` - Primary key for the version
- `forecast_id` - Foreign key to the main forecast record
- `version_number` - Sequential version number
- Same dimensional and forecast fields as the main table
- `created_at` - Timestamp when this version was created
- `comment` - Optional comment explaining the reason for this version

### Supporting Tables

#### 3. `wg_forecast.distributor_allocation_by_market_size_pack`

Lookup table that stores allocation percentages for distributing market-level forecasts to individual distributors.

**Key Fields:**
- `market_code` - Market identifier
- `customer_id` - Optional customer identifier
- `size_pack` - Product size/pack format
- `distributor_id` - Distributor identifier
- `distributor_allocation` - Percentage allocation for this distributor

## Core Functions

### 1. `save_forecast_version`

Creates a new version of an existing forecast and updates the current record.

**Parameters:**
- `p_forecast_id` - ID of the forecast to version
- Dimensional and forecast data fields
- `p_user_id` - ID of the user making the change
- `p_comment` - Optional comment describing the change

**Returns:** New version number

### 2. `revert_forecast_to_version`

Reverts a forecast to a previous version by creating a new version with the same data as the specified version.

**Parameters:**
- `p_forecast_id` - ID of the forecast to revert
- `p_version_number` - Version to revert to
- `p_user_id` - ID of the user performing the reversion
- `p_comment` - Optional comment (defaults to "Reverted to version X")

**Returns:** New version number

### 3. `get_forecast_history`

Retrieves the complete version history for a specific forecast.

**Parameters:**
- `p_forecast_id` - ID of the forecast to get history for

**Returns:** Table of version details ordered by version number descending

### 4. `smart_save_forecast`

Handles saving forecasts intelligently - creates new forecasts or versions existing ones as appropriate.

**Parameters:**
- Dimensional and forecast data fields
- `p_user_id` - ID of the user making the change
- `p_comment` - Optional comment describing the change

**Returns:** Forecast ID

### 5. `batch_save_forecasts_json`

Processes multiple forecasts from a JSON payload, handling allocation to distributors.

**Parameters:**
- `p_forecasts` - JSON array of forecast data
- `p_user_id` - ID of the user making the changes
- `p_forecast_status` - Status to set for the forecasts

**Returns:** Array of affected forecast IDs

## Complete Workflow

The forecast versioning system follows this workflow:

### 1. Client-Side JSON Preparation

The client application prepares a JSON array containing the forecast data to be saved:

```json
[
  {
    "market_code": "USATN1",
    "size_pack": "Tullamore Dew 12YO 6x750ML",
    "forecast_year": 2025,
    "month": 1,
    "forecast_method": "flat",
    "manual_case_equivalent_volume": 100.5,
    "comment": "Monthly adjustment"
  },
  // Additional forecast records...
]
```

Each JSON object represents a market-level forecast. Optional fields can include:
- `customer_id` - For customer-specific forecasts
- `state`, `market_name`, `distributor_name`, etc. - Additional dimensional data
- `brand`, `brand_id`, `variant`, `variant_id` - Product information

### 2. JSON Validation

When `batch_save_forecasts_json` receives the JSON payload, it first performs validation:

1. Checks for duplicate forecast records in the input based on business keys
2. Validates that all required fields are present and non-empty
3. If any validation fails, the entire transaction is aborted with an error message

### 3. Distributor Allocation

For each validated forecast record:

1. The function determines if a `customer_id` is provided
2. It queries the `distributor_allocation_by_market_size_pack` table to get allocation percentages for the market/size_pack combination
3. For each distributor with an allocation percentage:
   - Calculates allocated volume: `market_volume × distributor_allocation_percentage`
   - Passes the allocated data to `smart_save_forecast`

### 4. Smart Saving Logic

For each distributor-allocated forecast:

1. The `smart_save_forecast` function checks if a record with the same business keys already exists
2. If an existing record is found:
   - Calls `save_forecast_version` to create a new version
   - Updates the current record with the new data and increments the version number
3. If no existing record is found:
   - Creates a new record in the main forecast table
   - Creates an initial version (version 1) in the version history table

### 5. Version History Management

All changes are tracked in the version history table:

1. Each time a forecast is updated, a new row is added to the version history
2. The version history contains a complete copy of the forecast data at that point in time
3. The main forecast table always contains the latest version, with a reference to its current version number

### 6. Reversion Capability

If a user needs to revert to a previous version:

1. The `revert_forecast_to_version` function is called with the desired version number
2. It retrieves the data from that version
3. Creates a new version with that data (rather than directly modifying the current record)
4. This maintains an unbroken audit trail of all changes, including reversions

## Example Use Cases

### 1. Creating Initial Forecasts

When users create new forecasts:
- JSON data is sent to `batch_save_forecasts_json`
- The system creates new records with version 1
- Distributors receive allocated portions of the market forecast

### 2. Updating Existing Forecasts

When users modify existing forecasts:
- Updated values are sent via JSON
- The system creates new versions of the affected forecasts
- The current state is updated while preserving history

### 3. Reverting Changes

If users need to undo changes:
- They can view the version history of a forecast
- Select a previous version to revert to
- The reversion itself creates a new version rather than overwriting history

### 4. Audit Trail and Accountability

The system maintains a complete record of:
- What changes were made
- When changes were made
- Who made the changes
- The reason for changes (via comments)

## Conclusion

The Forecast Versioning System provides a robust, auditable mechanism for managing forecast data with complete version history. By utilizing a combination of current state and historical tables, along with specialized functions for managing versions, the system ensures data integrity while allowing flexibility in forecast management.

---

## Addendum: Method-Agnostic Manual Overrides (Depletions)

- Manual overrides persist regardless of selected forecast method. Storage and reads are normalized to a single row per key `(FGMD, MARKET_CODE, DISTRIBUTOR_ID, VARIANT_SIZE_PACK_ID, FORECAST_YEAR, MONTH)`.
- Write behavior:
  - `FORECAST.SP_BATCH_SAVE_FORECASTS` upsserts by the method-agnostic key and sets `FORECAST_METHOD='MANUAL'` on insert/update.
  - Duplicate detection ignores `forecast_method`.
  - Primary method updates remain independent in `DEPLETIONS_FORECAST_PRIMARY_FORECAST_METHOD`.
- Read behavior:
  - `vw_get_depletions_base` joins manual overrides by keys excluding method and exposes `OVERRIDDEN_CASE_EQUIVALENT_VOLUME = COALESCE(MANUAL_CASE_EQUIVALENT_VOLUME, CASE_EQUIVALENT_VOLUME)` and `IS_MANUAL_INPUT`.
- Consensus sync:
  - `_INTERNAL_SP_SYNC_CONSENSUS_TO_NEXT_MONTH` dedupes across methods (prefers `'MANUAL'`, else latest `UPDATED_AT`) and writes `'MANUAL'` in the target FGMD.
- Chains:
  - Chains manual inputs already follow method-agnostic semantics; no change required.