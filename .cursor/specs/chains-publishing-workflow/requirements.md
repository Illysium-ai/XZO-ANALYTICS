# Chains Publishing Workflow - Requirements

## Overview
This feature extends the existing forecast publishing workflow to support the "Chains" module, which handles outlet-level depletion forecasts in addition to the core volume module's distributor-level forecasts.

## User Stories

### Story 1: Chains Publication Integration
**As a** forecast manager  
**I want** the chains module to publish alongside the core volume module during consensus promotion  
**So that** both distributor-level and outlet-level forecasts are synchronized and available for review

### Story 2: Shared Publication Infrastructure  
**As a** system administrator  
**I want** chains publications to use the same group_id and publication_id as core volume  
**So that** both modules remain in lockstep and share the same publication governance

### Story 3: Chains Unpublishing Support
**As a** forecast manager  
**I want** to unpublish chains forecasts when core volume forecasts are unpublished  
**So that** the system maintains data consistency across all forecast modules

## Functional Requirements (EARS Format)

### FR-001: Chains Consensus Publishing
**WHEN** the core volume module is promoted from 'review' to 'consensus' status  
**THE SYSTEM SHALL** automatically publish both manual chains forecasts (MANUAL_INPUT_DEPLETIONS_FORECAST_CHAINS) and draft chains forecasts (DEPLETIONS_FORECAST_INIT_DRAFT_CHAINS) to DEPLETIONS_FORECAST_PUBLISHED_FORECASTS_CHAINS

### FR-002: Shared Publication Metadata
**WHEN** chains forecasts are published during consensus promotion  
**THE SYSTEM SHALL** use the same group_id and publication_id as the corresponding core volume publication

### FR-003: No Review-Level Chains Publishing  
**WHEN** the core volume module is published to 'review' status  
**THE SYSTEM SHALL NOT** publish chains forecasts (chains only publishes during consensus promotion)

### FR-004: Running Table Behavior
**WHEN** chains forecasts are published  
**THE SYSTEM SHALL** snapshot the current state of MANUAL_INPUT_DEPLETIONS_FORECAST_CHAINS regardless of forecast_generation_month_date (running table behavior)

### FR-005: Chains Division Unpublishing
**WHEN** a division's core volume forecasts are unpublished  
**THE SYSTEM SHALL** also remove any chains published forecasts associated with the same group_id and publication_id

### FR-006: Chains Market Unpublishing  
**WHEN** a market's core volume forecasts are unpublished  
**THE SYSTEM SHALL** also remove any chains published forecasts associated with the same publication_id

### FR-007: Publication Infrastructure Reuse
**WHEN** chains forecasts are published or unpublished  
**THE SYSTEM SHALL** use the existing DEPLETIONS_FORECAST_PUBLICATION_GROUPS and DEPLETIONS_FORECAST_PUBLICATIONS tables (no separate chains publication tables)

### FR-008: Parent Chain Data Handling
**WHEN** chains forecasts are published  
**THE SYSTEM SHALL** include parent chain identifiers (PARENT_CHAIN_CODE, PARENT_CHAIN_NAME) in the published forecasts table

### FR-009: Draft Chains Publishing
**WHEN** chains forecasts are published during consensus promotion  
**THE SYSTEM SHALL** also publish records from DEPLETIONS_FORECAST_INIT_DRAFT_CHAINS for the same forecast_generation_month_date where no manual input exists

## Non-Functional Requirements

### NFR-001: Performance
The chains publishing workflow SHALL complete within 30 seconds for a typical division (50 markets, 10,000 outlet-level forecast records)

### NFR-002: Data Consistency  
Chains and core volume publications SHALL always be created or failed together within the same transaction

### NFR-003: Backward Compatibility
The implementation SHALL NOT modify existing core volume publishing behavior

## Acceptance Criteria

### AC-001: Consensus Promotion Integration
- [ ] Chains forecasts are published when `sp_publish_division_forecast` promotes to consensus
- [ ] Chains forecasts are NOT published when `sp_publish_division_forecast` publishes to review
- [ ] Both core and chains use identical group_id and publication_id values

### AC-002: Unpublishing Integration  
- [ ] `sp_unpublish_division_forecast` removes both core and chains published forecasts
- [ ] `sp_unpublish_market_forecast` removes both core and chains published forecasts for the specified market
- [ ] Unpublishing operations are atomic (both succeed or both fail)

### AC-003: Data Integrity
- [ ] Published chains forecasts contain all required parent_chain-level fields
- [ ] Chains published forecasts reference the correct publication metadata
- [ ] No orphaned chains published forecasts exist after unpublishing operations

## Out of Scope
- Creation of new publication group or publication tables for chains
- Modification of chains manual input table structure  
- Independent chains publishing workflow (chains only publishes with core consensus promotion)
- Chains-specific publication status tracking 