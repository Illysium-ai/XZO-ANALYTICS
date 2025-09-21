# Apollo dbt Project

This dbt project models the alcohol industry data for William Grant & Sons using the three-tier system framework. It transforms raw data into analytics-ready views and provides a clear, maintainable structure for data modeling.

## Project Structure

- **Staging Models**: Clean and standardize source data
- **Intermediate Models**: Perform complex transformations and calculations
- **Mart Models**:
  - **Dimensional Models**: Store business entities (products, distributors, etc.)
  - **Fact Models**: Store business events and metrics (depletions, sales, etc.)
  - **Reporting Models**: Aggregate models optimized for specific reporting needs

## Business Logic Implementation

This project implements key alcohol industry metrics and concepts:

- **Three-Tier System**: Tracking flow between producers, distributors, and retailers
- **Case Equivalent Calculations**: Normalizing different container sizes to industry-standard units
- **Days of Supply**: Inventory management metric based on depletion rates
- **Channel Types**: On-premise vs. off-premise sales tracking
- **Product Hierarchy**: Category → Subcategory → Brand → Variant → SKU

## Macros

The project includes several macros to standardize calculations:

- **standardized_size**: Normalizes container sizes across products
- **case_equivalent_factor**: Calculates the case equivalent factor for different products
- **case_equivalent_type**: Determines the appropriate case equivalent type based on product category
- **days_of_supply**: Calculates inventory days of supply

## Getting Started

1. Clone this repository
2. Install dbt (`pip install dbt-postgres`)
3. Set up your profile in `~/.dbt/profiles.yml` or use the included profile
4. Run `dbt deps` to install dependencies
5. Run `dbt build` to build all models

## Documentation

Run `dbt docs generate && dbt docs serve` to view the documentation for this project.

## Testing

Run `dbt test` to run all tests for this project. 