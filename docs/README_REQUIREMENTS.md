# Core Volume Functionality - Data Requirements

## Business Requirements

"Ultimately the core volume functionality just needs a view where we can query this data at the market variant/size level and then append that against first party data (their forecast).
Then when they click on actual months the ability to go into a view that shows the raw data (sales at account level) by market / size variant.
Seems like most of it's there but just gotta make sure it cascades up to various product and customer/market hierarchies we want."

## Key Tables Identified

### Primary Sales/Volume Data Tables:

1. **VIP SRS Sales File Extract**
   - Contains invoice-level sales information from distributors
   - Essential for account-level sales data drill-down
   - Use for detailed actual sales data views

2. **VIP SRS Summary Depletions File Extract**
   - Contains summary sales and inventory information reported monthly
   - Ideal for aggregated market/variant level views
   - Use for top-level analysis and comparison with forecasts

3. **VIP SRS Summary Depletions Control File Extract**
   - Provides sales totals by distributor, item, and month
   - Useful for cross-verification of data
   - Can help ensure data completeness

### Market/Account Level Information:

4. **VIP Retail Outlet File Extract**
   - Contains geographic, demographic, and marketing details for retail outlets
   - Important for market-level analysis and segmentation
   - Provides context for sales data

5. **VIP Outlet Master File**
   - Contains detailed attributes of retail outlets (chains and independents)
   - Critical for mapping sales to specific market segments
   - Provides the base outlet information needed for rollups

### Hierarchies (Customer/Market/Product):

6. **VIPOUT Ownership Hierarchy File**
   - Details the ownership hierarchy from immediate owner to corporate parent
   - Allows rolling up account data to different market hierarchy levels
   - Essential for customer/market hierarchical views

7. **VIPOUT Ownership Description File**
   - Contains detailed information behind ownership codes
   - Complements the hierarchy file with descriptive information
   - Provides context for ownership relationships

8. **VIP Outlet Cross Reference File**
   - Maps supplier distributor IDs and distributor outlet IDs to VIP's unique outlet IDs
   - Crucial for integrating different data sources
   - Enables consistent identification across systems

## Database Table Names

The following table maps each data source to its corresponding database table name:

| Data Source | Table Name | Description |
|-------------|------------|-------------|
| VIP Chain Code File | VOCHAIN | Chain code definitions |
| VIP Class of Trade File | VOCOT | Class of trade classifications |
| VIP Field Values File | VIPVALUE | Field value definitions |
| VIP Outlet Cross Reference File | VOXREF | Outlet cross-reference mappings |
| VIP Outlet Master File | VIPOUT | Master outlet information |
| VIP SRS Calendar File Extract | SRSCAL | Calendar definitions |
| VIP SRS Chain File Extract | SRSCHAIN | Chain information |
| VIP SRS Distributor Master Extract | DISTDA | Distributor master data |
| VIP SRS Distributor Salesperson File Extract | SLSDA | Salesperson information |
| VIP SRS Future Sales File Extract | ORD | Future sales orders |
| VIP SRS Inventory File Extract | INVDA | Inventory data |
| VIP SRS Item File Extract | ITM_2_DA | Item master data |
| VIP SRS Non Reporters File Extract | NONDA | Non-reporting entities |
| VIP SRS Retail Outlet File Extract | OUTDA | Retail outlet data |
| VIP SRS Sales File Extract | SLSDA | Sales transaction data |
| VIP SRS Summary Depletions Control File Extract | CTLDA | Depletions control data |
| VIP SRS Summary Depletions File Extract | DEPLDA | Summary depletions data |
| VIP SRS Valid Values File Extract | SRLVALUE | Valid value definitions |
| VIP SRS distributor Item Cross Ref File Extract | ITMDA | Item cross-reference data |
| VIPOUT Ownership Description File | VOOWNDESC | Ownership descriptions |
| VIPOUT Ownership Hierarchy File | VOOWNHIER | Ownership hierarchy |

## Implementation Approach

1. Create base staging models for each key table with cleansed and standardized data

2. Develop a dimensional model with:
   - Fact tables based on Sales and Summary Depletions
   - Dimension tables for Outlets, Markets, Products, and Time
   - Hierarchies for Products and Customers/Markets

3. Build aggregated marts at market/variant/size level for comparison with forecast data

4. Create drill-down views to show account-level detail when users select specific months

5. Ensure all models support aggregation up various hierarchies to enable flexible analysis

## DBT Model Structure

The implementation follows the dbt model structure in `tenants/dbt_williamgrant/`:

- **Staging Models**: Clean and standardize source data from the tables above
- **Mart Models**: 
  - **Fact Models**: Sales facts, depletion facts, summary depletions base
  - **Master Models**: Product, outlet, distributor, and time dimensions
  - **Forecast Models**: Forecasting and trend analysis capabilities 