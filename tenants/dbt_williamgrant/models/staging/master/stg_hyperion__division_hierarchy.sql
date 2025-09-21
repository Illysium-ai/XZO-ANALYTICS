{{
  config(
    materialized = 'view'
  )
}}

-- Parse the JSON data for markets by division
WITH json_data AS (
  SELECT
    PARSE_JSON(data) AS markets_json
  FROM {{ source('public', 'util_data') }}
  WHERE table_name = 'markets_by_division'
)
, divisions_flattened AS (
  SELECT
    f1.key::VARCHAR AS division,
    f1.value AS sub_divisions_object
  FROM
    json_data,
    LATERAL FLATTEN(input => markets_json) f1
)
, sub_divisions_flattened AS (
  SELECT
    d.division,
    f2.key::VARCHAR AS sub_division,
    f2.value AS market_array
  FROM
    divisions_flattened d,
    LATERAL FLATTEN(input => d.sub_divisions_object) f2
)
, markets AS (
  SELECT
    sd.division,
    sd.sub_division,
    f3.value['market_id']::VARCHAR AS market_code,
    f3.value['market_name']::VARCHAR AS market_name
  FROM
    sub_divisions_flattened sd,
    LATERAL FLATTEN(input => sd.market_array) f3
)
SELECT
  m.division,
  m.sub_division,
  m.market_code,
  m.market_name
FROM markets m
ORDER BY m.division, m.sub_division, m.market_code