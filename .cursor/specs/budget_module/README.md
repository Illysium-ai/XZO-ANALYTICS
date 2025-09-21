# Budget Module - Comprehensive Guide

The Budget Module provides a complete workflow for generating, editing, and approving budgets based on published forecast baselines in the Apollo Analytics platform.

## Quick Start

### For Backend API Developers
1. **Read the API Reference**: Start with [`api_reference.md`](./api_reference.md) for complete procedure signatures and integration patterns
2. **Deploy Infrastructure**: Follow deployment steps in [`deployment.md`](../../../tenants/dbt_williamgrant/backend_functions/sf_budget_workflow/budget_deployment.md)
3. **Test Workflow**: Use the examples in the API reference to verify functionality

### For Frontend Developers
1. **Review Data Contracts**: See the table schema and UDTF return format in [`api_reference.md`](./api_reference.md)
2. **Understand State Management**: Budget data is method-agnostic with primary method preferences
3. **Handle Approval Locks**: Check approval status before enabling edit operations

## Architecture Overview

The Budget Module follows a **baseline-driven approach**:
1. **Select Baseline**: Choose a published forecast by `FORECAST_GENERATION_MONTH_DATE` (FGMD)
2. **Generate Y+1**: Create next-year budgets using forecast logic with baseline year as input
3. **Edit & Version**: Support method-agnostic user overrides with complete audit trails
4. **Approve & Lock**: Finalize budget cycles to prevent further changes

## Key Components

### Stored Procedures (API Endpoints)
- **`SP_GENERATE_BUDGET`**: Generate Y+1 budgets from baseline
- **`SP_BATCH_SAVE_BUDGETS`**: Save method-agnostic edits with versioning
- **`SP_APPROVE_BUDGET`**: Lock budget cycle
- **`UDTF_GET_DEPLETIONS_BUDGET`**: Read budget working set
- **`UDF_IS_BUDGET_APPROVED`**: Check approval status

### Data Model
- **Generated Data**: `DEPLETIONS_BUDGET_GENERATED` (hybrid table)
- **Manual Overrides**: `MANUAL_INPUT_DEPLETIONS_BUDGET` (hybrid table)
- **Version History**: `*_VERSIONS` tables (immutable audit trail)
- **Primary Methods**: `DEPLETIONS_BUDGET_PRIMARY_METHOD` (budget-specific)
- **Final View**: `VW_GET_BUDGET_BASE` (dbt model combining all sources)

### Method-Agnostic Design
- User edits apply to all forecast methods for the same monthly key
- Primary method preferences control default display
- No method required for volume overrides

## Implementation Status

✅ **Completed (Production Ready)**:
- All core stored procedures and functions
- Hybrid table DDL with proper indexing
- dbt models and source definitions
- Method-agnostic override logic
- Version history and audit trails
- Approval workflow and locking
- Comprehensive API documentation

📋 **Pending**:
- Performance testing at scale
- Security role definitions
- CI/CD integration
- Audit dashboard model

## Integration Checklist

### Backend Service Integration

- [ ] **Deploy Infrastructure**
  - [ ] Run DDL scripts in target environment
  - [ ] Deploy stored procedures and functions
  - [ ] Create dbt view dependencies
  - [ ] Configure proper grants and permissions

- [ ] **Implement API Wrappers**
  - [ ] Create service layer for procedure calls
  - [ ] Implement error handling and logging
  - [ ] Add input validation and sanitization
  - [ ] Configure connection pooling

- [ ] **Add Monitoring**
  - [ ] Track procedure execution times
  - [ ] Monitor approval workflow status
  - [ ] Alert on generation failures
  - [ ] Log user activity patterns

### Frontend Application Integration

- [ ] **State Management**
  - [ ] Handle approval lock states
  - [ ] Cache primary method preferences
  - [ ] Manage method-agnostic edit behavior
  - [ ] Track version history

- [ ] **User Interface**
  - [ ] Default to primary method view
  - [ ] Provide method switching capability
  - [ ] Show override indicators clearly
  - [ ] Display approval status prominently

- [ ] **Data Operations**
  - [ ] Implement pagination for large datasets
  - [ ] Batch multiple edits efficiently
  - [ ] Handle distributor allocation correctly
  - [ ] Provide version revert functionality

## Common Integration Patterns

### Reading Budget Data
```sql
-- Primary method only (recommended for initial load)
SELECT * FROM TABLE(
    FORECAST.UDTF_GET_DEPLETIONS_BUDGET(
        '2025-08-01'::DATE,  -- budget_cycle_date
        NULL::VARCHAR,       -- all methods
        ARRAY_CONSTRUCT('USANY1'), -- specific markets
        ARRAY_CONSTRUCT(),   -- all customers
        ARRAY_CONSTRUCT(),   -- all products
        TRUE                 -- primary only
    )
)
WHERE YEAR = 2026;
```

### Batch Editing
```json
// Method-agnostic edit format
[
  {
    "market_code": "USANY1",
    "customer_id": "12345",  // Optional
    "variant_size_pack_id": "VSP-001",
    "forecast_year": 2026,
    "month": 1,
    "manual_case_equivalent_volume": 150.0,
    "selected_forecast_method": "six_month",  // Optional
    "comment": "Seasonal adjustment"
  }
]
```

### Workflow Orchestration
```sql
-- 1. Generate budgets
CALL FORECAST.SP_GENERATE_BUDGET('2025-08-01', 'api_user');

-- 2. Check status before edits
SELECT FORECAST.UDF_IS_BUDGET_APPROVED('2025-08-01');

-- 3. Save edits (if not approved)
CALL FORECAST.SP_BATCH_SAVE_BUDGETS('[...]', '2025-08-01', 'user123');

-- 4. Approve when ready
CALL FORECAST.SP_APPROVE_BUDGET('2025-08-01', 'manager', 'Final approval', TRUE);
```

## Error Handling Guide

### Common Error Codes
- `-20021`: Budget cycle already approved (locked)
- `-20022`: No baseline data found for FGMD
- `-20011`: Cannot save - cycle is locked
- `-20012`: Duplicate records in batch JSON
- `-20013`: Missing required fields
- `-20014`: Missing volume update fields

### Debugging Strategies
1. **Check baseline availability** before generation
2. **Validate JSON structure** before batch saves
3. **Verify approval status** before edit attempts
4. **Monitor allocation tables** for distributor mapping issues

## Performance Considerations

### Read Operations
- Use filtering parameters to limit result sets
- Prefer primary-only queries for initial loads
- Consider pagination for large datasets
- Cache approval status checks

### Write Operations
- Batch multiple edits in single procedure calls
- Avoid frequent small updates
- Monitor version table growth
- Schedule cleanup of old versions

## Security & Compliance

### Access Control
- Budget read access: `ANALYST_ROLE`
- Budget write access: `BACKEND_ENG_ROLE`
- Approval permissions: Elevated roles only
- Multi-tenant isolation via FGMD selection

### Audit Requirements
- Complete change history in `*_VERSIONS` tables
- User identification in all operations
- Approval workflow with timestamps
- Immutable version records

## Documentation Index

1. **[`requirements.md`](./requirements.md)** - EARS format functional requirements
2. **[`design.md`](./design.md)** - Technical architecture and design decisions
3. **[`api_reference.md`](./api_reference.md)** - Complete API specifications and examples
4. **[`validation_script.md`](./validation_script.md)** - Comprehensive test suite to verify functionality
5. **[`tasks.md`](./tasks.md)** - Implementation task tracking and status
6. **[`budget_deployment.md`](../../../tenants/dbt_williamgrant/backend_functions/sf_budget_workflow/budget_deployment.md)** - Deployment scripts and examples

## Support & Troubleshooting

For implementation issues:
1. Check the [API Reference troubleshooting section](./api_reference.md#troubleshooting)
2. Review the [deployment guide](../../../tenants/dbt_williamgrant/backend_functions/sf_budget_workflow/budget_deployment.md)
3. Validate data model expectations in [`design.md`](./design.md)

The Budget Module is designed to be production-ready with comprehensive error handling, version management, and audit capabilities. All core functionality has been implemented and tested for the William Grant tenant.