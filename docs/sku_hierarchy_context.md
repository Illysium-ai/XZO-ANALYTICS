### How to Understand the Mapping:

- **Brand** (`BS31010 - Glenfiddich`) represents the highest level in your product hierarchy.

- **Variant** (`BS4GF001 - Glenfiddich 12`) is a specific expression of that brand—in this case, Glenfiddich 12-Year Whisky.

- **Size Pack** (`BS6GF001 - Glenfiddich 12`) represents the distribution configuration, such as how the product is physically packed and shipped (e.g., `12x100`, `12x75`, `24x37.5`, `6x175`, `96x5`).

- **Hyperion SKU** (`251775 - Glenfiddich 12 12x1L 40.0 CK WRD TR US`) is the detailed SKU identifier at a more granular level. It includes additional attributes:
  - **Size specifics**: Exact bottle size and pack count (`12x1L`, `12x750ml`, etc.)
  - **Alcohol content** (40.0 ABV)
  - **Market & packaging details** (e.g., CK WRD TR US, AG TTW Festive TR US), specifying nuances like special promotional packaging ("festive"), markets, or duty designation.

---

### Reasoning Explained:

- The SKU (`variant`) at the **variant level** (`BS4GF001 - Glenfiddich 12`) groups multiple size-pack configurations because it represents the actual liquid/product variant—Glenfiddich 12-Year Whisky—irrespective of the packaging format or marketing details.

- The **size pack** (`BS6GF001 - Glenfiddich 12`) provides a standardized identifier to group and manage inventory at the distribution level. It's common to see multiple detailed SKU representations ("hyperion_sku") mapped under the same size pack if they only differ by subtle attributes (e.g., market-specific labeling or minor packaging variations).

- The **hyperion SKU** is the most detailed level of granularity. Each hyperion SKU is uniquely defined, capturing:
  - Bottle dimensions (`1L`, `750ml`, `375ml`, `1.75L`, `50ml`)
  - Pack configurations (e.g., cases of 6, 12, or 96 units)
  - Additional packaging or branding details (`Festive TR`, `DIS TR`, etc.)
  - Region-specific variants (`US`, indicating market region)

### How You Should Understand It:

- **Brand Level**: Highest product grouping (Glenfiddich)
- **Variant Level**: Identifies the specific whisky variant (12-Year)
- **Size Pack**: Operational/logistics level grouping for how distributors manage shipments.
- **Hyperion SKU**: Granular detail—exact size, packaging, alcohol content, market-specific attributes. Each Hyperion SKU is unique and supports precise inventory control, regulatory compliance, and sales analytics.

### Practical Implications:
- If you're performing analytics or reporting on inventory or sales, **Hyperion SKU** is the most precise level.
- If you're assessing performance or demand at the brand or product variant level, you'd aggregate to the **variant** level.
- For operational efficiency, distributors typically manage logistics at the **size pack** level, consolidating different **Hyperion SKUs** under the same size-pack code to simplify processes.

This structure allows you to balance simplicity at higher-level reporting (brand and variant) and detailed tracking for operational needs (Hyperion SKU).

---

Yes, you will definitely need to normalize the SKU-level sales data before aggregating it at the **size pack** level. Here's why, along with a recommended approach:

### Why Normalization is Necessary:
Because different SKUs under the same **size pack** may represent varying quantities of liquid, aggregating raw sales volume without normalization would incorrectly imply equivalence between SKUs of significantly different sizes.

**For example**:

- **12x750ml** SKU has a total of **9,000 ml** per pack.
- **12x1L** SKU has a total of **12,000 ml** per pack.
- **6x1.75L** SKU has a total of **10,500 ml** per pack.

If you sum raw sales units directly, you'll misrepresent actual product performance since a case of 12x1L bottles isn't equivalent in volume or retail value to a case of 12x750ml bottles.

---

### Recommended Normalization Approach:

Normalize using a consistent unit, typically either:

1. **Liquid volume (e.g., liters or milliliters)**  
   - Convert each SKU into total milliliters or liters, aggregate at the size pack level, and then analyze.
   - Example:  
     ```
     SKU units sold × (bottles per case × ml per bottle)
     ```

2. **Standardized Case Equivalent**  
   - If the client has a preferred "standard case" size (commonly 9-liter case equivalents in alcohol distribution), normalize every SKU to that standard size before aggregating.

2. **Proof or ABV-adjusted basis** (less common, but useful in some scenarios)  
   - If relevant, you can normalize on ABV as well, though typically, pure liquid volume is sufficient for commercial analysis.

**Recommended approach for clarity**:

- Normalize to **Total Liters** per SKU sold, then aggregate at the **size pack** level.
- You'd calculate something like:
  ```
  Total Liters Sold per SKU = Units Sold × Bottles per pack × Volume per Bottle (L)
  ```

Then aggregate:
```sql
SELECT
    size_pack,
    SUM(total_liters_sold) AS total_liters_sold,
    SUM(sales_value) as total_sales_amount
FROM normalized_sku_sales
GROUP BY size_pack;
```

**Why normalization is important here**:

- Without normalization, smaller or larger configurations can disproportionately skew performance measures.
- Normalization allows accurate comparison and analysis of true performance at the "size pack" level, aligning with your client's requirements.

This approach ensures accurate, meaningful, and actionable insights, respecting the variation in product configurations within each "size pack" grouping.