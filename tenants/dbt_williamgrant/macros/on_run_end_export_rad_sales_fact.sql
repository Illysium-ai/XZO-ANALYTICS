{% macro on_run_end_export_rad_sales_fact() %}
{#
  Exports the rad_sales_fact model to S3 via the external stage @STG_WILLIAMGRANT_EXPORT
  under the path 'reconciliation' with a filename prefix 'slsda'.
  Runs only when target.name == 'prod'.
#}

  {% if target.name != 'prod' %}
    {{ log('Skipping rad_sales_fact export: non-prod target (' ~ target.name ~ ').', info=True) }}
    {% do return(none) %}
  {% endif %}

  {% set rad_sales_relation = ref('rad_sales_fact') %}
  {% set export_stage_name = 'APOLLO_WILLIAMGRANT.S3.STG_WILLIAMGRANT_EXPORT' %}
  {% set export_stage_path = 'reconciliation' %}
  {% set export_filename_prefix = 'slsda' %}

  {% set dated_prefix -%}
{{ export_filename_prefix }}_{{ run_started_at.strftime('%Y%m%d') }}
  {%- endset %}

  {% set copy_sql -%}
COPY INTO @{{ export_stage_name }}/{{ export_stage_path }}/{{ dated_prefix }}.csv.gz
FROM (
WITH rec_data AS (
SELECT
    concat(year,'-',month) AS year_month,
    month_date,
    year,
    market_name,
    concat(left(distributor_id,5),'-',right(distributor_id,3)) AS customer_id,
    sku_id AS partid,
    sku_description AS part_desc,
    sum(phys_quantity) AS phys_quantity,
    sum(case_equivalent_quantity) AS case_equivalent_quantity
FROM {{ rad_sales_relation }}
WHERE year = year(current_date)
GROUP BY ALL
)
SELECT
    *
FROM rec_data
ORDER BY year_month, market_name, customer_id, partid
)
FILE_FORMAT=(TYPE=CSV FIELD_DELIMITER=',' COMPRESSION=GZIP)
HEADER=TRUE
OVERWRITE=TRUE
SINGLE=TRUE
  {%- endset %}

  {{ log('Exporting rad_sales_fact to @' ~ export_stage_name ~ '/' ~ export_stage_path ~ ' with prefix ' ~ dated_prefix, info=True) }}
  {% do run_query(copy_sql) %}

  {% set list_sql -%}
LIST @{{ export_stage_name }}/{{ export_stage_path }}/{{ dated_prefix }}.csv.gz*
  {%- endset %}
  {{ log('Listing exported files at @' ~ export_stage_name ~ '/' ~ export_stage_path ~ '/' ~ dated_prefix ~ '*', info=True) }}
  {% do run_query(list_sql) %}

{% endmacro %}
