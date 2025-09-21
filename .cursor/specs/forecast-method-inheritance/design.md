# Forecast Method Inheritance — Design

## Architecture overview
- Introduce a market-level selection CTE in `depletions_forecast_primary_forecast_method.sql` that picks the prior FGMD method per `(market_code, variant_size_pack_id)` associated with the lowest distributor_id (proxy for pre-existing distributor).
- Use this CTE as a fallback when a distributor-level carry-over method does not exist for the new FGMD.
- Preserve current incremental behavior: keep existing rows; insert only missing rows for the FGMD.

## Data contracts / interfaces
- Input: `{{ ref('depletions_forecast_init_draft') }}` providing `data_source`, `market_code`, `distributor_id`, `variant_size_pack_id`, `forecast_generation_month_date`.
- Existing table: `{{ this }}` (`depletions_forecast_primary_forecast_method`) containing prior FGMD methods, used to derive market-level selection.
- Output: `forecast_method` for `(market_code, distributor_id, variant_size_pack_id, forecast_generation_month_date)` with `is_primary_forecast_method = 1`.

## Logic (ties to R-###)
- R-004 precedence: if `data_source = 'previous_consensus'`, set `'run_rate'`.
- Else, prefer distributor carry-over from previous FGMD (current join logic) — implicit.
- Else, use market-level method from previous FGMD tied to the minimum distributor_id (R-001, R-002).
- Else, default to `'six_month'` (R-003).

## Diagram
```mermaid
flowchart TD
  A[Init draft rows fci] --> B{data_source = 'previous_consensus'?}
  B -- yes --> M[method = 'run_rate']
  B -- no --> C[Left join prev FGMD distributor method]
  C -->|found| O[Use distributor carry-over]
  C -->|not found| D[Prev FGMD market/VSP via min(distributor_id)]
  D -->|found| O
  D -->|not found| E[Default 'six_month']
  O --> F[Insert missing rows for FGMD]
```

## Trade-offs / decisions
- Prioritizing the lowest distributor_id provides a deterministic “pre-existing distributor” heuristic aligned with operations.
- Using only prior FGMD maintains temporal consistency; avoids mixing months.
- Does not alter chains variant, which maintains its separate logic.

## Risks and mitigations
- Risk: `distributor_id` ordering is lexical; ensure IDs are consistently comparable. Mitigation: IDs are stable and deterministic; if needed, cast rules can be added later.
- Risk: Incremental joins could introduce duplicates; mitigated by existing unique key and left join filter.
- Risk: Performance impact from scanning `{{ this }}`; mitigate by grouping and windowing once per FGMD.
