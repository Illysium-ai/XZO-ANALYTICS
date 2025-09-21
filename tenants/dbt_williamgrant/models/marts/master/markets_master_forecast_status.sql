{{
  config(
    materialized = 'view'
  )
}}

SELECT
    mm.id,
    mm.market_name,
    mm.market_code,
    mm.market_hyperion,
    mm.market_coding,
    mm.market_id,
    mm.area_description,
    mm.customers,
    mm.settings,
    coalesce(df.forecast_generation_month_date, FORECAST.UDF_GET_VALID_FORECAST_GENERATION_MONTH_DATE()) as forecast_generation_month_date,
    coalesce(df.publication_status,'draft') as forecast_status
FROM
    {{ ref('markets_master') }} mm
LEFT JOIN (
    select
      market_code,
      publication_status,
      forecast_generation_month_date,
      approval_status_date
    from FORECAST.DEPLETIONS_FORECAST_PUBLICATIONS
    where forecast_generation_month_date = FORECAST.UDF_GET_VALID_FORECAST_GENERATION_MONTH_DATE()
    and publication_status in ('review','consensus')
) df
ON mm.market_id = df.market_code