# Budget Module API Reference

This document provides comprehensive API specifications for the Budget Module, enabling backend API services and frontend applications to interact with the budget workflows through Snowflake stored procedures and functions.

## Overview

The Budget Module provides a complete workflow for:
- Generating budgets from published forecast baselines
- Managing method-agnostic user edits with versioning
- Reading budget data with primary method support
- Approving and locking budget cycles

All procedures are deployed in the `FORECAST` schema and follow consistent patterns for error handling, versioning, and multi-tenant support.

## Core API Endpoints

### 1. Generate Budget (`SP_GENERATE_BUDGET`)

**Purpose**: Generates Y+1 budget data from a selected published forecast baseline (FGMD)

**Signature**:
```sql
CALL FORECAST.SP_GENERATE_BUDGET(
    P_BUDGET_CYCLE_DATE DATE,
    P_USER_ID VARCHAR
)
```

**Parameters**:
- `P_BUDGET_CYCLE_DATE`: The forecast generation month date (FGMD) from `DEPLETIONS_FORECAST_PUBLISHED_FORECASTS` to use as baseline
- `P_USER_ID`: User identifier for audit trails

**Returns**: `VARCHAR` status message

**Business Logic**:
- Uses baseline year (Y) as input for trend calculations only
- Generates 12 months of Y+1 data using same logic as `depletions_forecast_init_draft`
- Methods: `three_month`, `six_month`, `twelve_month`, `run_rate`, `flat`
- Automatically seeds budget-specific primary methods
- Zero-seeds planned products missing from baseline

**Error Conditions**:
- `-20021`: Budget cycle is already approved (locked)
- `-20022`: No baseline published forecasts found for the provided FGMD

**Example**:
```sql
-- Generate budget for 2026 using August 2025 baseline
CALL FORECAST.SP_GENERATE_BUDGET('2025-08-01'::DATE, 'api_user');
```

---

### 2. Read Budget Data (`UDTF_GET_DEPLETIONS_BUDGET`)

**Purpose**: Retrieves budget working set with method-agnostic overrides and primary method filtering

**Signature**:
```sql
SELECT * FROM TABLE(
    FORECAST.UDTF_GET_DEPLETIONS_BUDGET(
        P_BUDGET_CYCLE_DATE DATE,
        P_FORECAST_METHOD VARCHAR,
        P_MARKETS ARRAY,
        P_CUSTOMERS ARRAY, 
        P_VARIANT_SIZE_PACK_IDS ARRAY,
        P_ONLY_PRIMARY BOOLEAN DEFAULT NULL
    )
)
```

**Parameters**:
- `P_BUDGET_CYCLE_DATE`: Budget cycle date (FGMD) to read
- `P_FORECAST_METHOD`: Optional method filter (`three_month`, `six_month`, etc.). If NULL, returns all methods
- `P_MARKETS`: Optional array of market codes for filtering
- `P_CUSTOMERS`: Optional array of customer IDs (first 5 chars of distributor_id) for filtering  
- `P_VARIANT_SIZE_PACK_IDS`: Optional array of variant size pack IDs for filtering
- `P_ONLY_PRIMARY`: If TRUE, returns only primary method rows per key. If FALSE/NULL, returns all methods

**Returns**: Table with columns:
```sql
MARKET_ID VARCHAR,
MARKET VARCHAR,
MARKET_AREA_NAME VARCHAR,
CUSTOMER_ID VARCHAR,
CUSTOMER VARCHAR,
BRAND VARCHAR,
VARIANT VARCHAR,
VARIANT_ID VARCHAR,
VARIANT_SIZE_PACK_DESC VARCHAR,
VARIANT_SIZE_PACK_ID VARCHAR,
YEAR INTEGER,
MONTH INTEGER,
FORECAST_METHOD VARCHAR,
BUDGET_CYCLE_DATE DATE,
DATA_TYPE VARCHAR, -- Always 'budget'
IS_MANUAL_INPUT BOOLEAN,
FORECAST_STATUS VARCHAR, -- 'draft' or 'approved'
CURRENT_VERSION INTEGER,
COMMENT VARCHAR,
CASE_EQUIVALENT_VOLUME FLOAT,
PY_CASE_EQUIVALENT_VOLUME FLOAT,
CY_3M_CASE_EQUIVALENT_VOLUME FLOAT,
CY_6M_CASE_EQUIVALENT_VOLUME FLOAT,
CY_12M_CASE_EQUIVALENT_VOLUME FLOAT,
PY_3M_CASE_EQUIVALENT_VOLUME FLOAT,
PY_6M_CASE_EQUIVALENT_VOLUME FLOAT,
PY_12M_CASE_EQUIVALENT_VOLUME FLOAT,
GSV_RATE FLOAT -- Currently NULL
```

**Business Logic**:
- Manual overrides take precedence over generated values
- Method-agnostic overrides apply to all methods for the same monthly key
- Primary method preference uses budget-specific primary method table
- Results are aggregated and rounded to 2 decimal places

**Example**:
```sql
-- Get primary method data only for specific markets
SELECT * FROM TABLE(
    FORECAST.UDTF_GET_DEPLETIONS_BUDGET(
        '2025-08-01'::DATE,
        NULL::VARCHAR,
        ARRAY_CONSTRUCT('USANY1', 'USATN1'),
        ARRAY_CONSTRUCT(),
        ARRAY_CONSTRUCT(),
        TRUE
    )
)
WHERE YEAR = 2026
LIMIT 100;
```

---

### 3. Batch Save Budgets (`SP_BATCH_SAVE_BUDGETS`)

**Purpose**: Saves method-agnostic manual overrides and updates primary methods with full versioning

**Signature**:
```sql
CALL FORECAST.SP_BATCH_SAVE_BUDGETS(
    P_BUDGETS_JSON VARCHAR,
    P_BUDGET_CYCLE_DATE DATE, 
    P_USER_ID VARCHAR
)
```

**Parameters**:
- `P_BUDGETS_JSON`: JSON array of budget edits (see format below)
- `P_BUDGET_CYCLE_DATE`: Budget cycle date (FGMD) being edited
- `P_USER_ID`: User identifier for audit trails

**JSON Format**:
```json
[
  {
    "market_code": "USANY1",
    "customer_id": "12345", // Optional - if provided, edits specific customer; if null, edits all distributors in market
    "variant_size_pack_id": "VSP-001",
    "forecast_year": 2026,
    "month": 1,
    "manual_case_equivalent_volume": 123.45, // Optional - if provided, creates/updates override
    "selected_forecast_method": "six_month", // Optional - if provided, updates primary method
    "comment": "Monthly adjustment"
  }
]
```

**Returns**: `VARCHAR` status message

**Business Logic**:
- Method-agnostic volume edits: An override applies to all forecast methods for the same monthly key
- Distributor allocation: Market-level edits are allocated across distributors using `DISTRIBUTOR_ALLOCATION_BY_MARKET_SIZE_PACK`
- Primary method updates: Optional `selected_forecast_method` updates the budget-specific primary method
- Versioning: Every change creates a new version in `MANUAL_INPUT_DEPLETIONS_BUDGET_VERSIONS`
- Batch validation: Checks for duplicates, missing fields, and approval locks before processing

**Error Conditions**:
- `-20011`: Budget cycle is approved (locked)
- `-20012`: Duplicate budget data found in JSON
- `-20013`: Missing required field (market_code)
- `-20014`: Missing required fields for volume update

**Example**:
```sql
CALL FORECAST.SP_BATCH_SAVE_BUDGETS($$
[
  {
    "market_code": "USANY1",
    "variant_size_pack_id": "VSP-001", 
    "forecast_year": 2026,
    "month": 1,
    "manual_case_equivalent_volume": 150.0,
    "selected_forecast_method": "six_month",
    "comment": "Increased for promotion"
  }
]
$$, '2025-08-01'::DATE, 'api_user');
```

---

### 4. Approve/Lock Budget (`SP_APPROVE_BUDGET`)

**Purpose**: Approves and locks a budget cycle, preventing further edits. Also supports unlocking for advanced users.

**Signature**:
```sql
CALL FORECAST.SP_APPROVE_BUDGET(
    P_BUDGET_CYCLE_DATE DATE,
    P_APPROVED_BY_USER_ID VARCHAR,
    P_APPROVAL_NOTE TEXT,
    P_LOCK BOOLEAN
)
```

**Parameters**:
- `P_BUDGET_CYCLE_DATE`: Budget cycle date to approve/unlock
- `P_APPROVED_BY_USER_ID`: User identifier performing the action
- `P_APPROVAL_NOTE`: Optional approval/unlock note
- `P_LOCK`: TRUE to approve and lock, FALSE to unlock

**Returns**: `VARCHAR` status message

**Business Logic**:
- When `P_LOCK = TRUE`: Creates approval record with timestamp and approver, locks editing
- When `P_LOCK = FALSE`: Removes approval record, enables editing (admin function)
- Prevents duplicate approvals
- Gracefully handles unlock requests when already unlocked

**Error Conditions**:
- `-20001`: Budget cycle is already approved (when trying to approve already approved cycle)

**Examples**:
```sql
-- Approve and lock budget cycle
CALL FORECAST.SP_APPROVE_BUDGET(
    '2025-08-01'::DATE,
    'budget_manager',
    'Final budget approved for production',
    TRUE
);

-- Unlock budget cycle (admin function)
CALL FORECAST.SP_APPROVE_BUDGET(
    '2025-08-01'::DATE,
    'admin_user',
    'Unlock for emergency changes',
    FALSE
);
```

---

### 5. Check Approval Status (`UDF_IS_BUDGET_APPROVED`)

**Purpose**: Checks if a budget cycle is approved (locked)

**Signature**:
```sql
SELECT FORECAST.UDF_IS_BUDGET_APPROVED(P_BUDGET_CYCLE_DATE DATE)
```

**Parameters**:
- `P_BUDGET_CYCLE_DATE`: Budget cycle date to check

**Returns**: `BOOLEAN` - TRUE if approved, FALSE if not

**Example**:
```sql
SELECT FORECAST.UDF_IS_BUDGET_APPROVED('2025-08-01'::DATE) AS is_locked;
```

## Data Model Reference

### Core Tables

**`FORECAST.DEPLETIONS_BUDGET_GENERATED`** (Hybrid Table)
- Generated Y+1 budget data across all methods
- Populated by `SP_GENERATE_BUDGET`
- Key: `(BUDGET_CYCLE_DATE, MARKET_CODE, DISTRIBUTOR_ID, VARIANT_SIZE_PACK_ID, FORECAST_YEAR, MONTH, FORECAST_METHOD)`

**`FORECAST.MANUAL_INPUT_DEPLETIONS_BUDGET`** (Hybrid Table)  
- Method-agnostic manual overrides
- Updated by `SP_BATCH_SAVE_BUDGETS`
- Key: `(BUDGET_CYCLE_DATE, MARKET_CODE, DISTRIBUTOR_ID, VARIANT_SIZE_PACK_ID, FORECAST_YEAR, MONTH)`

**`FORECAST.MANUAL_INPUT_DEPLETIONS_BUDGET_VERSIONS`** (Hybrid Table)
- Immutable version history of all changes
- Append-only audit trail

**`FORECAST.DEPLETIONS_BUDGET_PRIMARY_METHOD`** (Hybrid Table)
- Budget-specific primary method per key
- Seeded during generation, user-editable until approval
- Key: `(BUDGET_CYCLE_DATE, MARKET_CODE, DISTRIBUTOR_ID, VARIANT_SIZE_PACK_ID)`

### Views

**`FORECAST.VW_GET_BUDGET_BASE`** (dbt view)
- Combines generated data with manual overrides
- Method-agnostic precedence: manual override when present, else generated value
- Used by UDTF for final budget reads

## Integration Patterns

### Backend API Service Integration

**Typical Workflow**:
1. **Generate**: Call `SP_GENERATE_BUDGET` with selected FGMD
2. **Read**: Use `UDTF_GET_DEPLETIONS_BUDGET` with primary-only filter for initial UI load
3. **Edit**: Collect user changes and call `SP_BATCH_SAVE_BUDGETS` 
4. **Approve**: Call `SP_APPROVE_BUDGET` when cycle is final

**Error Handling**:
- All procedures return structured error messages
- Check approval status before edit operations
- Validate JSON format before batch operations

**Performance Considerations**:
- Use filtering parameters in UDTF for large datasets
- Batch multiple edits in single `SP_BATCH_SAVE_BUDGETS` call
- Consider pagination for read operations

### Frontend Application Integration

**State Management**:
- Track approval status to disable edit capabilities
- Cache primary method preferences per key
- Handle method-agnostic edits correctly (apply to all methods)

**User Experience**:
- Default to primary method view for simplicity
- Allow method switching for power users
- Show override indicators clearly
- Provide version history access

**Data Binding**:
```javascript
// Example API call structure
const budgetData = await callSnowflake(`
  SELECT * FROM TABLE(
    FORECAST.UDTF_GET_DEPLETIONS_BUDGET(
      '${cycleDate}',
      NULL,
      ARRAY_CONSTRUCT(${markets.map(m => `'${m}'`).join(',')}),
      ARRAY_CONSTRUCT(),
      ARRAY_CONSTRUCT(),
      TRUE
    )
  )
  WHERE YEAR = ${targetYear}
`);

// Batch edit structure
const edits = [
  {
    market_code: "USANY1",
    variant_size_pack_id: "VSP-001",
    forecast_year: 2026,
    month: 1, 
    manual_case_equivalent_volume: 150.0,
    selected_forecast_method: "six_month",
    comment: "User adjustment"
  }
];

await callSnowflake(`
  CALL FORECAST.SP_BATCH_SAVE_BUDGETS(
    '${JSON.stringify(edits)}',
    '${cycleDate}',
    '${userId}'
  )
`);
```

## Security & Access Control

**Required Permissions**:
- `USAGE` on `FORECAST` schema
- `SELECT` on source tables and views
- `EXECUTE` on stored procedures and functions
- `INSERT`, `UPDATE`, `DELETE` on hybrid tables (for procedures)

**Multi-Tenant Isolation**:
- Data is isolated by `BUDGET_CYCLE_DATE` (FGMD) selection
- Market-level filtering provides additional scoping
- No cross-tenant data access in procedures

**Audit Trail**:
- All procedures log user actions with timestamps
- Complete version history in `*_VERSIONS` tables
- Approval workflow creates permanent audit records

## Troubleshooting

**Common Issues**:

1. **No baseline data**: Ensure FGMD exists in `DEPLETIONS_FORECAST_PUBLISHED_FORECASTS` with `consensus` status
2. **Permission errors**: Verify schema grants and table permissions
3. **JSON validation errors**: Check JSON syntax and required fields in batch operations
4. **Lock conflicts**: Check approval status before edit attempts
5. **Allocation errors**: Ensure `DISTRIBUTOR_ALLOCATION_BY_MARKET_SIZE_PACK` has data for target markets

**Debugging Queries**:
```sql
-- Check available baselines
SELECT DISTINCT FORECAST_GENERATION_MONTH_DATE, MIN(FORECAST_YEAR) as baseline_year
FROM FORECAST.DEPLETIONS_FORECAST_PUBLISHED_FORECASTS pf
JOIN FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS p ON pf.PUBLICATION_ID = p.PUBLICATION_ID  
WHERE p.PUBLICATION_STATUS = 'consensus'
GROUP BY 1 ORDER BY 1 DESC;

-- Check approval status
SELECT BUDGET_CYCLE_DATE, APPROVED_BY, APPROVED_AT, COMMENT
FROM FORECAST.DEPLETIONS_BUDGET_APPROVALS
ORDER BY APPROVED_AT DESC;

-- Check recent edits
SELECT MARKET_CODE, VARIANT_SIZE_PACK_ID, FORECAST_YEAR, MONTH, 
       MANUAL_CASE_EQUIVALENT_VOLUME, UPDATED_BY_USER_ID, UPDATED_AT
FROM FORECAST.MANUAL_INPUT_DEPLETIONS_BUDGET
WHERE BUDGET_CYCLE_DATE = '2025-08-01'
  AND UPDATED_AT >= CURRENT_TIMESTAMP() - INTERVAL '1 hour'
ORDER BY UPDATED_AT DESC;
```

## Version History

- **v1.0**: Initial implementation with core generate/read/edit/approve workflow
- **Current**: All Phase 1 tasks completed, Phase 2 dbt models implemented, Phase 3 validation in progress

For implementation details and architectural decisions, see:
- [`design.md`](./design.md) - Detailed technical architecture
- [`requirements.md`](./requirements.md) - EARS format requirements  
- [`tasks.md`](./tasks.md) - Implementation task tracking