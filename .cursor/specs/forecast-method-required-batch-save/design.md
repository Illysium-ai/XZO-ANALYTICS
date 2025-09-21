# Design: Forecast Method Required in Batch Save Budgets

## Overview
Add strict validation for `forecast_method` in `FORECAST.SP_BATCH_SAVE_BUDGETS` during pre-flight checks. This is a localized change within the stored procedure and does not alter downstream tables or interfaces beyond stricter input validation.

## Data Contract
Each element in `P_BUDGETS_JSON` must include:
- `market_code`: string, non-empty
- `forecast_method`: string, one of: `three_month`, `six_month`, `twelve_month`, `flat`, `run_rate`

Other fields are unchanged (e.g., `manual_case_equivalent_volume`, `variant_size_pack_id`, `forecast_year`, `month`, optional `customer_id`, etc.).

## Implementation Points
- Update Pre-flight Check 2 to validate:
  - Presence and non-empty `market_code` (existing)
  - Presence and non-empty `forecast_method`
  - Membership of `forecast_method` in the allowed set
- Update the `missing_required_fields_ex` message to reflect both required fields and enumerate allowed values.
- Do not alter MERGE logic, versions insert, or the primary method update semantics.

## Diagram
```mermaid
description: Batch save validation flow
sequenceDiagram
    participant Client
    participant Proc as SP_BATCH_SAVE_BUDGETS
    Client->>Proc: Invoke with P_BUDGETS_JSON
    Proc->>Proc: Pre-flight checks (market_code, forecast_method)
    alt Invalid
        Proc-->>Client: Raise -20013 missing_required_fields_ex
    else Valid
        Proc->>Proc: MERGE manual budget rows
        Proc->>Proc: Insert version rows
        Proc->>Proc: Update primary method (if provided)
        Proc-->>Client: SUCCESS message
    end
```

## Trade-offs / Decisions
- Enforce strict lowercase canonical values to avoid ambiguity across clients.
- Keep validation localized to a single pre-flight block for performance and clarity.

## Risks / Mitigations
- Risk: Breaking change for clients not sending `forecast_method`.
  - Mitigation: Clear error messaging; coordinate rollout; add client-side validation.
- Risk: Typo/case mismatches.
  - Mitigation: Document allowed values; consider client-side enums.

## Traceability
- Implements R-001, R-002, R-003. Preserves R-004 (no behavior change beyond validation).
