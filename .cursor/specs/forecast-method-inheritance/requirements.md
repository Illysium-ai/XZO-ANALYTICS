# Forecast Method Inheritance — Requirements

- R-001 — WHEN initializing primary forecast methods for a new distributor for a given `(market_code, variant_size_pack_id, forecast_generation_month_date)`, THE SYSTEM SHALL inherit the market-level forecast method previously used for that `(market_code, variant_size_pack_id)` in the most recent prior forecast generation month.
  - Acceptance: For a new `(market_code, distributor_id, variant_size_pack_id)` combination on FGMD N, if any distributors existed on FGMD N-1 with `(market_code, variant_size_pack_id)`, the new distributor’s method equals the selected market-level method.

- R-002 — WHEN multiple methods existed across distributors in the prior month for the same `(market_code, variant_size_pack_id)`, THE SYSTEM SHALL choose the method associated with the lowest distributor_id among those prior distributors (proxy for “pre-existing distributor”).
  - Acceptance: Given multiple methods, the chosen method is the one tied to the minimum distributor_id from the prior FGMD for that market/VSP.

- R-003 — WHEN no prior-month market-level method exists for the `(market_code, variant_size_pack_id)`, THE SYSTEM SHALL default to `'six_month'`.
  - Acceptance: In markets/VSPs with no prior distributor rows, the initial method resolves to `'six_month'` for new distributors.

- R-004 — WHEN `data_source = 'previous_consensus'` on the base forecast rows, THE SYSTEM SHALL set the potential method to `'run_rate'` regardless of market-level selection.
  - Acceptance: Any row with `data_source = 'previous_consensus'` initializes as `'run_rate'`.

- R-005 — WHEN running incrementally, THE SYSTEM SHALL not overwrite existing methods for rows already present in the primary method table for the same FGMD.
  - Acceptance: Re-running the model for the same FGMD does not change existing method assignments; only missing rows are inserted.

- R-006 — WHEN performing a full-refresh is disabled (current config), THE SYSTEM SHALL remain compatible with current materialization strategy and not introduce new dependencies outside the model tree.
  - Acceptance: dbt run completes without schema or dependency errors under current settings; lineage remains unchanged.
