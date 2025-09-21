# Chains Publishing Workflow - Design

## Architecture Overview

The chains publishing workflow extends the existing forecast publishing system by integrating outlet-level forecast publishing with the core volume module's consensus promotion process.

### System Architecture

```
┌─────────────────────────┐    ┌─────────────────────────┐
│   Core Volume Module    │    │    Chains Module        │
│                         │    │                         │
│ MANUAL_INPUT_DEPLETIONS │    │ MANUAL_INPUT_DEPLETIONS │
│ _FORECAST               │    │ _FORECAST_CHAINS        │
│ (per FGMD)              │    │ (running table)         │
└─────────┬───────────────┘    └─────────┬───────────────┘
          │                              │
          │ Publish to Review            │ (No action)
          ▼                              │
┌─────────────────────────┐              │
│ DEPLETIONS_FORECAST_    │              │
│ PUBLISHED_FORECASTS     │              │
└─────────┬───────────────┘              │
          │                              │
          │ Promote to Consensus         │ Publish on Consensus
          ▼                              ▼
┌─────────────────────────┐    ┌─────────────────────────┐
│ DEPLETIONS_FORECAST_    │    │ DEPLETIONS_FORECAST_    │
│ PUBLISHED_FORECASTS     │    │ PUBLISHED_FORECASTS_    │
│ (updated to consensus)  │    │ CHAINS (new records)    │
└─────────┬───────────────┘    └─────────┬───────────────┘
          │                              │
          └──────────────┬───────────────┘
                         │
                    Shared Metadata
                         ▼
          ┌─────────────────────────┐
          │ DEPLETIONS_FORECAST_    │
          │ PUBLICATIONS &          │
          │ PUBLICATION_GROUPS      │
          └─────────────────────────┘
```

### Key Design Principles

1. **Shared Publication Infrastructure**: Chains uses the same publication groups and publications tables as core volume
2. **Consensus-Only Publishing**: Chains only publishes during consensus promotion, not review publication
3. **Running Table Behavior**: Chains snapshots current state regardless of FGMD, unlike core volume's FGMD-locked approach
4. **Atomic Operations**: Core and chains publishing/unpublishing operations are transactionally linked

## Data Flow Design

### Table Relationships

```
DEPLETIONS_FORECAST_PUBLICATION_GROUPS (shared)
├── GROUP_ID (Primary Key)
├── DIVISION
├── FORECAST_GENERATION_MONTH_DATE
└── ... other fields

DEPLETIONS_FORECAST_PUBLICATIONS (shared)  
├── PUBLICATION_ID (Primary Key)
├── GROUP_ID (Foreign Key)
├── MARKET_CODE
├── PUBLICATION_STATUS
└── ... other fields

DEPLETIONS_FORECAST_PUBLISHED_FORECASTS (core volume)
├── PUBLICATION_ID (Foreign Key)
├── SOURCE_TABLE = 'manual' | 'draft'
├── DISTRIBUTOR_ID
└── ... distributor-level fields

DEPLETIONS_FORECAST_PUBLISHED_FORECASTS_CHAINS (existing)
├── PUBLICATION_ID (Foreign Key) 
├── SOURCE_TABLE = 'chains'
├── DISTRIBUTOR_ID
├── PARENT_CHAIN_CODE ← Key difference
├── PARENT_CHAIN_NAME ← Key difference  
└── ... parent chain-level fields
```

### Schema for DEPLETIONS_FORECAST_PUBLISHED_FORECASTS_CHAINS

```sql
-- Already exists in snowflake_forecast_editing_tables_ddl_chains.sql
CREATE OR REPLACE TABLE APOLLO_WILLIAMGRANT.FORECAST.DEPLETIONS_FORECAST_PUBLISHED_FORECASTS_CHAINS (
    ID BIGINT PRIMARY KEY DEFAULT APOLLO_WILLIAMGRANT.FORECAST.SEQ_PUBLISHED_FORECASTS_CHAINS_ID.NEXTVAL,
    GROUP_ID INTEGER,
    PUBLICATION_ID INTEGER NOT NULL,
    SOURCE_TABLE VARCHAR(50) NOT NULL,
    SOURCE_ID INTEGER,
    MARKET_NAME VARCHAR(100),
    MARKET_CODE VARCHAR(50) NOT NULL,
    DISTRIBUTOR_NAME VARCHAR(100),
    DISTRIBUTOR_ID VARCHAR(50) NOT NULL,
    PARENT_CHAIN_NAME VARCHAR(100), -- Chains-specific
    PARENT_CHAIN_CODE VARCHAR(50) NOT NULL, -- Chains-specific
    BRAND VARCHAR(100),
    BRAND_ID VARCHAR(50),
    VARIANT VARCHAR(100),
    VARIANT_ID VARCHAR(50),
    VARIANT_SIZE_PACK_DESC VARCHAR(100),
    VARIANT_SIZE_PACK_ID VARCHAR(50) NOT NULL,
    FORECAST_YEAR INTEGER NOT NULL,
    MONTH INTEGER NOT NULL,
    FORECAST_METHOD VARCHAR(50) NOT NULL,
    FORECAST_GENERATION_MONTH_DATE DATE NOT NULL,
    CASE_EQUIVALENT_VOLUME FLOAT NOT NULL,
    VERSION_NUMBER INTEGER,
    PUBLISHED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (FORECAST_GENERATION_MONTH_DATE, MARKET_CODE, FORECAST_METHOD);
```

## Sequence Diagrams

### Consensus Promotion with Chains

```
User                 sp_publish_division_    Core Tables           Chains Tables
│                    forecast                                      
│ promote to         │                       │                     │
│ consensus         │                       │                     │
├───────────────────►│                       │                     │
│                    │ 1. Update core pubs   │                     │
│                    │   to consensus        │                     │
│                    ├──────────────────────►│                     │
│                    │                       │                     │
│                    │ 2. Archive core       │                     │
│                    │   manual & draft      │                     │
│                    ├──────────────────────►│                     │
│                    │                       │                     │
│                    │ 3. Archive chains     │                     │
│                    │   manual forecasts    │                     │
│                    ├─────────────────────────────────────────────►│
│                    │                       │                     │
│                    │ 4. Archive chains     │                     │
│                    │   draft forecasts     │                     │
│                    ├─────────────────────────────────────────────►│
│                    │                       │                     │
│                    │ 5. Sync consensus     │                     │
│                    │   to next month       │                     │
│                    ├──────────────────────►│                     │
│                    │                       │                     │
│                    │ 6. Return success     │                     │
│◄───────────────────┤                       │                     │
```

### Division Unpublishing with Chains

```
User                 sp_unpublish_division_  Core Tables           Chains Tables
│                    forecast                                      
│ unpublish          │                       │                     │
│ division          │                       │                     │
├───────────────────►│                       │                     │
│                    │ 1. Update pubs to     │                     │
│                    │   unpublished         │                     │
│                    ├──────────────────────►│                     │
│                    │                       │                     │
│                    │ 2. Delete core        │                     │
│                    │   published forecasts │                     │
│                    ├──────────────────────►│                     │
│                    │                       │                     │
│                    │ 3. Delete chains      │                     │
│                    │   published forecasts │                     │
│                    ├─────────────────────────────────────────────►│
│                    │                       │                     │
│                    │ 4. Update manual      │                     │
│                    │   statuses to draft   │                     │
│                    ├──────────────────────►│                     │
│                    │                       │                     │
│                    │ 5. Return success     │                     │
│◄───────────────────┤                       │                     │
```

## Integration Points

### Modified Procedures

1. **sp_publish_division_forecast.sql**
   - Add chains publishing logic in consensus promotion section (TWO INSERT statements):
     - Archive manual chains forecasts (from MANUAL_INPUT_DEPLETIONS_FORECAST_CHAINS)
     - Archive draft chains forecasts (from DEPLETIONS_FORECAST_INIT_DRAFT_CHAINS) where no manual override exists
   - Maintain existing review publishing behavior (no chains)
   - Ensure transactional consistency

2. **sp_unpublish_division_forecast.sql**  
   - Add chains unpublishing logic for division-level operations
   - Maintain transaction boundaries

3. **sp_unpublish_market_forecast.sql**
   - Add chains unpublishing logic for market-level operations
   - Handle outlet-level data cleanup

### No Additional DDL Required

- `DEPLETIONS_FORECAST_PUBLISHED_FORECASTS_CHAINS` table already exists
- `DEPLETIONS_FORECAST_PRIMARY_FORECAST_METHOD_CHAINS` table already exists  
- Tables have appropriate indexes and clustering configuration

## Error Handling Strategy

1. **Transaction Boundaries**: All core and chains operations within same transaction
2. **Rollback Strategy**: If chains publishing fails, rollback entire consensus promotion
3. **Validation**: Verify chains data exists before attempting to publish
4. **Logging**: Add specific logging for chains operations within existing procedure logging

## Performance Considerations

1. **Bulk Operations**: Use set-based INSERT statements for chains publishing
2. **Index Strategy**: Optimize for publication_id and market-level lookups  
3. **Clustering**: Consider clustering on publication_id for large datasets
4. **Parallel Processing**: Core and chains operations can be sequential (not parallel) due to shared metadata dependency

## Testing Strategy

1. **Unit Tests**: Test chains publishing in isolation
2. **Integration Tests**: Test full publish/unpublish workflows with both modules
3. **Performance Tests**: Validate performance with realistic data volumes
4. **Rollback Tests**: Verify transaction rollback scenarios 