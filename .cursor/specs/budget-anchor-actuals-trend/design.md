# Design — Budget Anchor at P_BUDGET_CYCLE_DATE, Actuals-Driven Trend Factors

## Overview
- Anchor date equals `P_BUDGET_CYCLE_DATE` (R-001).
- Domain is PF-driven: PF consensus for the cycle defines the keyspace, filtered by planned!=false, exclusions, and eligible distributors (R-009).
- Trend factors (3/6/12) and run-rate derived from CY/PY actuals using a 12‑month calendar ending at the anchor (R-002, R-003).
- Apply constant factors across all 12 months of target year `Y+1` by scaling CY published consensus volumes (R-004).
- Generate months `Jan..Dec` of `Y+1` regardless of anchor (R-005).
- Validate published consensus baseline exists (R-006).

## Data Sources / Contracts
- VIP.RAD_DISTRIBUTOR_LEVEL_SALES: `month_date`, `market_code`, `distributor_id`, `variant_size_pack_id`, `case_equivalent_quantity`, plus descriptive fields.
- FORECAST.DEPLETIONS_FORECAST_PUBLISHED_FORECASTS joined to FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS (status `consensus`) for CY monthly volumes and domain.
- MASTER_DATA.APOLLO_VARIANT_SIZE_PACK_TAG: planned flag and exclusion arrays; only `is_planned = 'false'` is excluded.
- MASTER_DATA.APOLLO_DIST_MASTER: distributor eligibility.

## Algorithm
1. Derive `Y = EXTRACT(YEAR FROM P_BUDGET_CYCLE_DATE)`; set `ANCHOR = P_BUDGET_CYCLE_DATE`, `PY_ANCHOR = DATEADD(YEAR, -1, ANCHOR)`.
2. Domain: take PF consensus keys for the cycle; filter planned!=false, apply market/customer exclusions, require eligible distributors.
3. Calendarize the 12 months ending at `ANCHOR`; left join RAD for CY and prior-year months for PY; treat missing months as 0.
4. Factors at anchor month: `trend_factor_k = LEAST(COALESCE(cy_k / NULLIF(py_k,0), 1.0), 1.5)`; `run_rate_3m = cy_3m / 3`.
5. Generate `fm` months for `Y+1` → `2026-01..2026-12` when `Y=2025`.
6. For each key, compute 4 method rows per month:
   - `three_month`, `six_month`, `twelve_month`: `volume = CY_published_volume(Y, month) * factor_k`.
   - `run_rate`: constant `run_rate_3m`.

## Diagram
```mermaid
sequenceDiagram
  participant Caller
  participant SP as SP_GENERATE_BUDGET
  participant VIP as VIP.RAD_DISTRIBUTOR_LEVEL_SALES
  participant PF as FORECAST.PUBLISHED_FORECASTS

  Caller->>SP: CALL(..., P_BUDGET_CYCLE_DATE, P_USER_ID)
  SP->>SP: Validate lock; ensure consensus baseline exists (R-006)
  SP->>PF: Load PF domain keys (planned!=false; exclusions; eligibility)
  SP->>VIP: Read CY/PY actuals via 12-month calendar ending at anchor (R-002)
  SP->>SP: Compute 3/6/12 factors; run_rate (R-002,R-003)
  SP->>PF: Read CY published consensus volumes (R-004)
  SP->>SP: Apply factors across months Jan..Dec of Y+1 (R-004,R-005)
  SP-->>Caller: SUCCESS
```

## Decisions and Trade-offs
- Domain from PF guarantees baseline alignment and full coverage; RAD remains the source of truth for factors.
- Calendarized windows avoid anchor-month presence requirements and produce stable factors.
- Clamp factors at 1.5 to limit extreme swings (unchanged behavior).

## Risks / Mitigations
- Missing PF for the cycle → explicit exception (R-006).
- Keys with PY window = 0 → factors default to 1.0 (existing clamp path).

## Traceability
- R-001 → Anchor variables; joins on `ANCHOR`.
- R-002,R-003 → Calendarized RAD windows and factor calcs.
- R-004 → Application to CY published volumes.
- R-005 → `fm` month generation for `Y+1`.
- R-006 → Baseline existence check.
- R-009 → PF-driven domain and planned/exclusion/eligibility filtering.
