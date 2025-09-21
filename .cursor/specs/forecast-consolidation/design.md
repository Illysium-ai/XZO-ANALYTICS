---
title: Design – Consolidate Init Draft with Monthend Projection
status: draft
include: always
---

### Design Overview
Consolidate month-end projection into init-draft models so FGMD month uses actuals-to-date projection, with control markets exempted. Extract projection into a reusable macro to eliminate duplicated logic across chain and non-chain grains and optionally keep standalone monthend models as thin wrappers.

### Current State (simplified)
- Init-draft models forecast 12 months starting the month after the latest complete month. The FGMD month is forecasted via PY*trend/run-rate rather than using actuals-to-date.
- Monthend prediction models produce projected full-month volumes for recent months using daily invoice progress.

### Target State
- For FGMD only, init-draft models consume the same projection logic as monthend models at the appropriate grain.
- Control markets (by `distributor_area`) continue current init-draft logic for FGMD.

### Key Components
- Macro: `forecast__monthend_projection(
    source_model,           -- relation (rad_invoice_level_sales or rad_distributor_level_sales)
    grain,                  -- 'chain' | 'distributor'
    target_year INT,        -- rd.forecast_generation_year
    target_month INT        -- rd.forecast_generation_month
  ) -> table (keys + projected_case_equivalent_quantity, projection_method_used)
`

- Grain Keys
  - Distributor: `market_code, distributor_id, variant_size_pack_id`
  - Chain: `market_code, distributor_id, parent_chain_code, variant_size_pack_id`

- Control Market Detection
  - Use `distributor_area` from the chosen source model
  - Control areas configurable: `var('control_distributor_areas', ['Control Market'])`

### Data Flow
1) Existing init-draft flow builds `run_details` with FGMD context.
2) Compute `fgmd_prediction` via macro, filtered to `(target_year, target_month)`.
3) Add `distributor_area` to upstream CTEs (propagate from RAD source).
4) In `forecast_rows`, override the FGMD month’s `case_equivalent_volume` with `fgmd_prediction.projected_case_equivalent_quantity` for non-control markets. For other months keep current logic.
5) Replicate FGMD value across all `forecast_method` to preserve shape.
6) Keep `actual_rows` unchanged. Preserve clustering, unique key, hooks.

### Pseudo-SQL Integration (non-chains)
```
with ... run_details as (...),
     ... rolling_sums as (...),
     fgmd_prediction as (
       select market_code, distributor_id, variant_size_pack_id,
              projected_case_equivalent_quantity,
              projection_method_used
       from {{ forecast__monthend_projection(
                ref('rad_distributor_level_sales'),
                'distributor',
                rd.forecast_generation_year,
                rd.forecast_generation_month) }}
     )
select ...
     case when fmt.forecast_month_date = rd.forecast_generation_month_date
               and coalesce(sd.distributor_area, '') not in (select value from table(flatten(input => parse_json('{{ var('control_distributor_areas', "['Control Market']") }}'))))
          then coalesce(fp.projected_case_equivalent_quantity,
                        /* fallback to current method */
                        case when tfu.forecast_method = 'run_rate' then coalesce(tfu.trend_factor, 1.0)
                             else coalesce(pya.case_equivalent_depletions, 0.0) * coalesce(tfu.trend_factor, 1.0)
                        end)
          else /* current method */
               case when tfu.forecast_method = 'run_rate' then coalesce(tfu.trend_factor, 1.0)
                    else coalesce(pya.case_equivalent_depletions, 0.0) * coalesce(tfu.trend_factor, 1.0)
               end
     end as case_equivalent_volume
from trend_factors_unpivoted tfu
... left join fgmd_prediction fp using (market_code, distributor_id, variant_size_pack_id)
...;
```

### Chains Variant
- Same integration, but join keys add `parent_chain_code` and macro is invoked with `source_model = ref('rad_invoice_level_sales')` and `grain = 'chain'`.

### Contracts
- No breaking schema changes in init-draft outputs
- Projection macro returns minimally: keys + `projected_case_equivalent_quantity`, optionally `projection_method_used` for diagnostics

### Traces to Requirements
- R-001/R-003: FGMD-only override using projection macro
- R-002: `distributor_area`-based exception
- R-004: Preserve shape via method replication
- R-005/R-006: One macro, two grains; standalone models call the macro
- R-007: Macro constrained to target month when used by init-draft
- R-009: Backfill supported by existing `backfill_fgmd`

### Alternatives & Trade-offs
- A) Keep two systems: simpler now, continued duplication (rejected)
- B) Inline projection SQL into both init-draft models: faster to ship but duplicated logic (risk of drift)
- C) Macro-based consolidation (recommended): single source of truth, thin wrappers for standalone monthend tables

### Risks & Mitigations
- Risk: `distributor_area` values vary; Mitigation: configurable allowlist via var
- Risk: Performance at scale; Mitigation: FGMD-only filtering and reuse existing indices/clustering
- Risk: Mid-month refresh expectations; Mitigation: documented `backfill_fgmd` job to re-materialize FGMD as needed

### References
- Init-draft non-chains: #[[file:apollo-analytics/tenants/dbt_williamgrant/models/marts/forecast/depletions_forecast_init_draft.sql]]
- Init-draft chains: #[[file:apollo-analytics/tenants/dbt_williamgrant/models/marts/forecast/depletions_forecast_init_draft_chains.sql]]
- Monthend prediction (non-chains): #[[file:apollo-analytics/tenants/dbt_williamgrant/models/marts/forecast/depletions_forecast_monthend_prediction.sql]]
- Monthend prediction (chains): #[[file:apollo-analytics/tenants/dbt_williamgrant/models/marts/forecast/depletions_forecast_monthend_prediction_chains.sql]]
