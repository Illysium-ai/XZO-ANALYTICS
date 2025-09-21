{% macro forecast__monthend_projection(grain, mode='fgmd', window_months=1) %}
-- grain: 'distributor' | 'chain'
-- mode: 'fgmd' uses rd.forecast_generation_month_date; 'window' uses last N months from control_date
-- window_months: number of months to generate when mode == 'window'
(
with control_date as (
    select max(invoice_date) as effective_current_date
    from {{ ref('rad_invoice_level_sales') }}
),

-- Target months to process
{% if mode == 'fgmd' %}
run_month as (
    select date_trunc('month', rd.forecast_generation_month_date)::date as month_start_date
    from run_details rd
),
{% else %}
run_month as (
    select date_trunc('month', c.effective_current_date)::date as month_start_date
    from control_date c
),
{% endif %}

{% if mode == 'window' %}
-- Last N months window ending at the month in run_month
target_processing_months as (
    select
        dateadd('month', row_number() over (order by 1) - {{ window_months }}, rm.month_start_date)::date as month_start_date
    from run_month rm,
         table(generator(rowcount => {{ window_months }}))
),
{% else %}
-- Single target month (FGMD)
target_processing_months as (
    select month_start_date from run_month
),
{% endif %}

-- Calendar for each processing month
date_generator as (
    select 
        tpm.month_start_date,
        row_number() over (partition by tpm.month_start_date order by 1) - 1 as day_offset
    from target_processing_months tpm,
         table(generator(rowcount => 50))
),
month_calendar_details as (
    select
        year(date_in_month)::integer as processing_year,
        month(date_in_month)::integer as processing_month,
        sum(case when dayofweek(date_in_month) between 1 and 5 then 1 else 0 end) as total_business_days_in_month,
        max(case when dayofweek(date_in_month) between 1 and 5 then date_in_month else null end) as last_business_day_of_month
    from (
        select 
            dg.month_start_date,
            dateadd('day', dg.day_offset, dg.month_start_date) as date_in_month
        from date_generator dg
        where dateadd('day', dg.day_offset, dg.month_start_date) < dateadd('month', 1, dg.month_start_date)
    )
    group by 1, 2
),

-- Prepare sales data at invoice level for the window and previous year
sales_data_prepared as (
    select
        s.year + 1 as processing_year,
        s.month as processing_month,
        s.invoice_date,
        dateadd('day', 364, s.invoice_date)::date as comparison_date,
        'previous_year' as period_type,
        s.market_name,
        s.market_code,
        s.distributor_id,
        s.distributor_name,
        {% if grain == 'chain' %}
        s.vip_parent_chain_name as parent_chain_name,
        s.vip_parent_chain_code as parent_chain_code,
        {% endif %}
        s.variant_size_pack_id,
        s.phys_quantity::number(38,8) as phys_quantity,
        s.case_equivalent_quantity::number(38,8) as case_equivalent_quantity
    from {{ ref('rad_invoice_level_sales') }} s
    where (s.year, s.month) in (
        select year(month_start_date) - 1, month(month_start_date)
        from target_processing_months
    )
    {% if grain == 'chain' %}
      and s.vip_parent_chain_code is not null and s.vip_parent_chain_code != '' and s.vip_parent_chain_code != '33333'
    {% endif %}
    union all
    select
        s.year as processing_year,
        s.month as processing_month,
        s.invoice_date,
        s.invoice_date as comparison_date,
        'current_year' as period_type,
        s.market_name,
        s.market_code,
        s.distributor_id,
        s.distributor_name,
        {% if grain == 'chain' %}
        s.vip_parent_chain_name as parent_chain_name,
        s.vip_parent_chain_code as parent_chain_code,
        {% endif %}
        s.variant_size_pack_id,
        s.phys_quantity::number(38,8) as phys_quantity,
        s.case_equivalent_quantity::number(38,8) as case_equivalent_quantity
    from {{ ref('rad_invoice_level_sales') }} s
    where (s.year, s.month) in (
        select year(month_start_date), month(month_start_date)
        from target_processing_months
    )
    {% if grain == 'chain' %}
      and s.vip_parent_chain_code is not null and s.vip_parent_chain_code != '' and s.vip_parent_chain_code != '33333'
    {% endif %}
),

-- Market-level consistent progress date within the target month(s)
market_level_progress as (
    select
        processing_year,
        processing_month,
        market_code,
        {% if grain == 'chain' %} parent_chain_code, {% endif %}
        variant_size_pack_id,
        max(invoice_date) as market_max_invoice_date
    from sales_data_prepared
    where period_type = 'current_year'
    group by 1,2,3{% if grain == 'chain' %},4{% endif %},{% if grain == 'chain' %}5{% else %}4{% endif %}
),

-- Aggregate by grain
aggregated_sales_metrics as (
    select
        p.processing_year,
        p.processing_month,
        p.market_name,
        p.market_code,
        p.distributor_id,
        p.distributor_name,
        {% if grain == 'chain' %}
        p.parent_chain_name,
        p.parent_chain_code,
        {% endif %}
        p.variant_size_pack_id,
        mlp.market_max_invoice_date,
        sum(case when p.period_type = 'current_year' then p.phys_quantity else 0 end) as current_month_actual_phys_quantity_todate,
        sum(case when p.period_type = 'current_year' then p.case_equivalent_quantity else 0 end) as current_month_actual_ceq_todate,
        sum(case when p.period_type = 'previous_year' then p.phys_quantity else 0 end) as prev_year_full_month_phys_quantity,
        sum(case when p.period_type = 'previous_year' then p.case_equivalent_quantity else 0 end) as prev_year_full_month_ceq,
        sum(case when p.period_type = 'previous_year' and p.comparison_date <= mlp.market_max_invoice_date then p.phys_quantity else 0 end) as prev_year_partial_phys_quantity,
        sum(case when p.period_type = 'previous_year' and p.comparison_date <= mlp.market_max_invoice_date then p.case_equivalent_quantity else 0 end) as prev_year_partial_ceq
    from sales_data_prepared p
    join market_level_progress mlp
      on p.processing_year = mlp.processing_year
     and p.processing_month = mlp.processing_month
     and p.market_code = mlp.market_code
     and p.variant_size_pack_id = mlp.variant_size_pack_id
     {% if grain == 'chain' %}
     and p.parent_chain_code = mlp.parent_chain_code
     {% endif %}
    group by all
),

-- Final calculations with business-day math
final_calculations as (
    select
        agg.*,
        cal.total_business_days_in_month,
        cal.last_business_day_of_month,
        case
            when date_from_parts(agg.processing_year, agg.processing_month, 1) < date_trunc('month', c.effective_current_date) then true
            else false
        end as month_is_fully_past,
        agg.prev_year_full_month_phys_quantity / nullif(agg.prev_year_partial_phys_quantity, 0) as factor_phys_quantity,
        agg.prev_year_full_month_ceq / nullif(agg.prev_year_partial_ceq, 0) as factor_ceq,
        datediff('day', date_trunc('month', agg.market_max_invoice_date), agg.market_max_invoice_date) + 1 
          - floor((datediff('day', date_trunc('month', agg.market_max_invoice_date), agg.market_max_invoice_date) + dayofweek(date_trunc('month', agg.market_max_invoice_date)) - 1) / 7) * 2 
          - case when dayofweek(date_trunc('month', agg.market_max_invoice_date)) = 0 then 1 else 0 end
          - case when dayofweek(agg.market_max_invoice_date) = 6 then 1 else 0 end as business_days_elapsed,
        agg.current_month_actual_phys_quantity_todate / nullif(
            datediff('day', date_trunc('month', agg.market_max_invoice_date), agg.market_max_invoice_date) + 1 
            - floor((datediff('day', date_trunc('month', agg.market_max_invoice_date), agg.market_max_invoice_date) + dayofweek(date_trunc('month', agg.market_max_invoice_date)) - 1) / 7) * 2 
            - case when dayofweek(date_trunc('month', agg.market_max_invoice_date)) = 0 then 1 else 0 end
            - case when dayofweek(agg.market_max_invoice_date) = 6 then 1 else 0 end, 0
        ) as avg_phys_qty_per_biz_day,
        agg.current_month_actual_ceq_todate / nullif(
            datediff('day', date_trunc('month', agg.market_max_invoice_date), agg.market_max_invoice_date) + 1 
            - floor((datediff('day', date_trunc('month', agg.market_max_invoice_date), agg.market_max_invoice_date) + dayofweek(date_trunc('month', agg.market_max_invoice_date)) - 1) / 7) * 2 
            - case when dayofweek(date_trunc('month', agg.market_max_invoice_date)) = 0 then 1 else 0 end
            - case when dayofweek(agg.market_max_invoice_date) = 6 then 1 else 0 end, 0
        ) as avg_ceq_per_biz_day
    from aggregated_sales_metrics agg
    join month_calendar_details cal
      on agg.processing_year = cal.processing_year and agg.processing_month = cal.processing_month
    cross join control_date c
),

-- Final projection per entity-month
final_output_generation as (
    select
        f.processing_year,
        f.processing_month,
        date_from_parts(f.processing_year, f.processing_month, 1) as month_date,
        f.market_name,
        f.market_code,
        f.distributor_id,
        f.distributor_name,
        {% if grain == 'chain' %}
        f.parent_chain_name,
        f.parent_chain_code,
        {% endif %}
        f.variant_size_pack_id,
        case
            when f.month_is_fully_past or f.market_max_invoice_date >= f.last_business_day_of_month then 'actuals'
            when f.factor_ceq is not null and f.factor_ceq > 0 then
                case
                    when coalesce(f.prev_year_full_month_ceq, 0) > 0
                         and (f.current_month_actual_ceq_todate * f.factor_ceq) > (2 * f.prev_year_full_month_ceq)
                         and (
                            f.current_month_actual_ceq_todate + (coalesce(f.avg_ceq_per_biz_day, 0) * greatest(0, (f.total_business_days_in_month - f.business_days_elapsed)))
                         ) <= (2 * f.prev_year_full_month_ceq)
                    then 'straight_line_avg_biz_day'
                    else 'factor_based'
                end
            else 'straight_line_avg_biz_day'
        end as projection_method_used,
        case
            when f.month_is_fully_past or f.market_max_invoice_date >= f.last_business_day_of_month then f.current_month_actual_ceq_todate
            when f.factor_ceq is not null and f.factor_ceq > 0 then
                case
                    when coalesce(f.prev_year_full_month_ceq, 0) > 0 then
                        case
                            when (f.current_month_actual_ceq_todate * f.factor_ceq) <= (2 * f.prev_year_full_month_ceq)
                                then (f.current_month_actual_ceq_todate * f.factor_ceq)
                            when (
                                f.current_month_actual_ceq_todate + (coalesce(f.avg_ceq_per_biz_day, 0) * greatest(0, (f.total_business_days_in_month - f.business_days_elapsed)))
                            ) <= (2 * f.prev_year_full_month_ceq)
                                then (
                                    f.current_month_actual_ceq_todate + (coalesce(f.avg_ceq_per_biz_day, 0) * greatest(0, (f.total_business_days_in_month - f.business_days_elapsed)))
                                )
                            else (2 * f.prev_year_full_month_ceq)
                        end
                    else (f.current_month_actual_ceq_todate * f.factor_ceq)
                end
            else
                case
                    when coalesce(f.prev_year_full_month_ceq, 0) > 0 then least(
                        f.current_month_actual_ceq_todate + (coalesce(f.avg_ceq_per_biz_day, 0) * greatest(0, (f.total_business_days_in_month - f.business_days_elapsed))),
                        (2 * f.prev_year_full_month_ceq)
                    )
                    else f.current_month_actual_ceq_todate + (coalesce(f.avg_ceq_per_biz_day, 0) * greatest(0, (f.total_business_days_in_month - f.business_days_elapsed)))
                end
        end as projected_case_equivalent_quantity,
        case
            when f.month_is_fully_past or f.market_max_invoice_date >= f.last_business_day_of_month then f.current_month_actual_phys_quantity_todate
            when f.factor_phys_quantity is not null and f.factor_phys_quantity > 0 then
                case
                    when coalesce(f.prev_year_full_month_phys_quantity, 0) > 0 then
                        case
                            when (f.current_month_actual_phys_quantity_todate * f.factor_phys_quantity) <= (2 * f.prev_year_full_month_phys_quantity)
                                then (f.current_month_actual_phys_quantity_todate * f.factor_phys_quantity)
                            when (
                                f.current_month_actual_phys_quantity_todate + (coalesce(f.avg_phys_qty_per_biz_day, 0) * greatest(0, (f.total_business_days_in_month - f.business_days_elapsed)))
                            ) <= (2 * f.prev_year_full_month_phys_quantity)
                                then (
                                    f.current_month_actual_phys_quantity_todate + (coalesce(f.avg_phys_qty_per_biz_day, 0) * greatest(0, (f.total_business_days_in_month - f.business_days_elapsed)))
                                )
                            else (2 * f.prev_year_full_month_phys_quantity)
                        end
                    else (f.current_month_actual_phys_quantity_todate * f.factor_phys_quantity)
                end
            else
                case
                    when coalesce(f.prev_year_full_month_phys_quantity, 0) > 0 then least(
                        f.current_month_actual_phys_quantity_todate + (coalesce(f.avg_phys_qty_per_biz_day, 0) * greatest(0, (f.total_business_days_in_month - f.business_days_elapsed))),
                        (2 * f.prev_year_full_month_phys_quantity)
                    )
                    else f.current_month_actual_phys_quantity_todate + (coalesce(f.avg_phys_qty_per_biz_day, 0) * greatest(0, (f.total_business_days_in_month - f.business_days_elapsed)))
                end
        end as projected_phys_quantity,
        case when (case
            when f.month_is_fully_past or f.market_max_invoice_date >= f.last_business_day_of_month then 'actuals'
            when f.factor_ceq is not null and f.factor_ceq > 0 then
                case
                    when coalesce(f.prev_year_full_month_ceq, 0) > 0
                         and (f.current_month_actual_ceq_todate * f.factor_ceq) > (2 * f.prev_year_full_month_ceq)
                         and (
                            f.current_month_actual_ceq_todate + (coalesce(f.avg_ceq_per_biz_day, 0) * greatest(0, (f.total_business_days_in_month - f.business_days_elapsed)))
                         ) <= (2 * f.prev_year_full_month_ceq)
                    then 'straight_line_avg_biz_day'
                    else 'factor_based'
                end
            else 'straight_line_avg_biz_day'
        end) = 'actuals' then false else true end as is_projected,
        -- Diagnostics and drivers
        f.current_month_actual_phys_quantity_todate,
        f.current_month_actual_ceq_todate,
        f.prev_year_partial_phys_quantity as prev_year_partial_phys_quantity_for_projection,
        f.prev_year_partial_ceq as prev_year_partial_ceq_for_projection,
        f.prev_year_full_month_phys_quantity as prev_year_full_month_phys_quantity_for_projection,
        f.prev_year_full_month_ceq as prev_year_full_month_ceq_for_projection,
        f.factor_phys_quantity,
        f.factor_ceq,
        f.total_business_days_in_month,
        f.last_business_day_of_month,
        f.business_days_elapsed,
        f.avg_phys_qty_per_biz_day,
        f.avg_ceq_per_biz_day,
        f.month_is_fully_past,
        f.market_max_invoice_date
    from final_calculations f
)

select
    fog.processing_year,
    fog.processing_month,
    fog.month_date,
    fog.month_date as forecast_month_date,
    fog.market_name,
    fog.market_code,
    fog.distributor_id,
    fog.distributor_name,
    {% if grain == 'chain' %} fog.parent_chain_name, fog.parent_chain_code, {% endif %}
    fog.variant_size_pack_id,
    fog.projected_case_equivalent_quantity,
    fog.projected_phys_quantity,
    fog.is_projected,
    fog.projection_method_used,
    fog.current_month_actual_phys_quantity_todate,
    fog.current_month_actual_ceq_todate,
    fog.prev_year_partial_phys_quantity_for_projection,
    fog.prev_year_partial_ceq_for_projection,
    fog.prev_year_full_month_phys_quantity_for_projection,
    fog.prev_year_full_month_ceq_for_projection,
    fog.factor_phys_quantity,
    fog.factor_ceq,
    fog.total_business_days_in_month,
    fog.last_business_day_of_month,
    fog.business_days_elapsed,
    fog.avg_phys_qty_per_biz_day,
    fog.avg_ceq_per_biz_day,
    fog.month_is_fully_past,
    fog.market_max_invoice_date
from final_output_generation fog
{% if mode == 'fgmd' %}
where fog.month_date = (select date_trunc('month', rd.forecast_generation_month_date)::date from run_details rd)
{% endif %}
order by fog.month_date, fog.market_code, fog.distributor_id, fog.variant_size_pack_id
)
{% endmacro %}
