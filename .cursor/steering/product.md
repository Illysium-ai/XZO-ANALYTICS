### Product Overview

- **Product**: Apollo Analytics — multi-tenant analytics and operational data platform on Snowflake supporting forecasting, budgeting, master data, and product tagging.
- **Target users**: Data engineers, analytics engineers, operations analysts, and application services consuming standardized Snowflake UDF/UDTF/SP interfaces.
- **Tenancy & environments**:
  - Tenants under `tenants/` (e.g., `dbt_williamgrant`, `dbt_acme`, `dbt_hotaling`)
  - Environments follow Snowflake databases per tenant (e.g., `APOLLO_DEVELOPMENT`, `APOLLO_WILLIAMGRANT`), with schemas such as `MASTER_DATA`, `FORECAST`.
- **Core capabilities**:
  - Forecast editing and publishing workflows (synchronous and batch)
  - Budget workflows (save/approve, UDF/UDTF support)
  - Product tagging and custom product management for `variant_size_pack_id` (VSPID)
  - Master data integration and shared marts for downstream analytics
- **Business objectives**:
  - Consistent data contracts and predictable stored procedure interfaces
  - Reliability and auditability with transactional changes
  - Performance via set-based Snowflake patterns
- **Data consumers**: Internal applications, BI dashboards, and ad-hoc analytics using stable schemas and functions.
- **Non-goals**: UI workflows, identity/auth changes, or tag taxonomy governance beyond storage and retrieval.

- **Feature focus — VSP tag clear behavior**:
  - When `tag_names` is present as an empty array, tags for that VSP are cleared (set to empty arrays).
  - When `tag_names` is omitted, existing tags remain unchanged.
  - This preserves explicit user intent while keeping omission as a no-op.
