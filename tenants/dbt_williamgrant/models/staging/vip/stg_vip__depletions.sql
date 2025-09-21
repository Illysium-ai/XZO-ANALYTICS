{{
  config(
    materialized = 'view'
  )
}}

with staged as (
  select
    -- Primary keys and identifiers
    left(dist_id,5)::VARCHAR as customer_id,
    dist_id::VARCHAR as distributor_id,
    depl_year_mon::VARCHAR as year_month,
    depletion_period::VARCHAR as period,
    supplier_item::VARCHAR as sku_id,
    dist_item::VARCHAR as distributor_item_id,
    
    -- Time components
    floor(depletion_period::INTEGER / 100)::INTEGER as year,
    mod(depletion_period::INTEGER, 100)::INTEGER as month,
    
    -- Inventory positions
    coalesce(begin_on_hand, 0)::NUMBER(10,2) as beginning_inventory,
    coalesce(receipts, 0)::NUMBER(10,2) as inventory_receipts,
    coalesce(trans_in, 0)::NUMBER(10,2) as inventory_trans_in,
    coalesce(trans_out, 0)::NUMBER(10,2) as inventory_trans_out,
    coalesce(returns, 0)::NUMBER(10,2) as inventory_returns,
    coalesce(breakage, 0)::NUMBER(10,2) as inventory_breakage,
    coalesce(samples, 0)::NUMBER(10,2) as inventory_samples,
    coalesce(adjustments, 0)::NUMBER(10,2) as inventory_adjustments,
    coalesce(total_inv_chg, 0)::NUMBER(10,2) as inventory_total_chg,
    coalesce(end_on_hand, 0)::NUMBER(10,2) as ending_inventory,
    coalesce(on_order, 0)::NUMBER(10,2) as on_order_inventory,
    
    -- Sales breakdowns (depletions)
    coalesce(total_sales, 0)::NUMBER(10,2) as total_depletions,
    coalesce(non_retail, 0)::NUMBER(10,2) as non_retail_depletions,
    coalesce(off_premise, 0)::NUMBER(10,2) as off_premise_depletions,
    coalesce(on_premise, 0)::NUMBER(10,2) as on_premise_depletions,
    coalesce(military_on, 0)::NUMBER(10,2) as military_on_premise,
    coalesce(military_off, 0)::NUMBER(10,2) as military_off_premise,
    coalesce(sub_dist_sales, 0)::NUMBER(10,2) as sub_distributor_sales,
    coalesce(transport, 0)::NUMBER(10,2) as transport_depletions,
    coalesce(un_classified, 0)::NUMBER(10,2) as unclassified_depletions,
    
    -- Days of supply calculation
    case 
        when coalesce(total_sales, 0) = 0 then null
        else (coalesce(end_on_hand, 0) / (coalesce(total_sales, 0) / 30.0))::NUMBER(10,1)
    end as days_of_supply,
    
    -- Audit information
    audit_date::BIGINT as audit_date,
    audit_user::VARCHAR as audit_user,
    out_audit_date::BIGINT as out_audit_date,
    out_audit_user::VARCHAR as out_audit_user,
    
    -- Fields for composite key
    alt_dist_id::VARCHAR as alt_dist_id
  from {{ source('vip', 'deplda') }}
)

select * from staged