# Supply Chain Optimization & Inventory Analytics

A full-stack analytics platform demonstrating advanced SQL, Python, and data visualization for inventory optimization and supplier performance analysis.

## Key Features

| Module | What It Does | Techniques Used |
|--------|--------------|-----------------|
| ABC Classification | Identifies which products drive 80% of revenue | Pareto analysis, cumulative percentages |
| Inventory Turnover | Detects fast movers vs dead stock | COGS/inventory ratio, days of supply |
| Reorder Optimization | Calculates when to reorder each product | Safety stock formulas, service levels |
| EOQ Analysis | Determines optimal order quantities | Wilson's formula, cost minimization |
| Supplier Scoring | Ranks suppliers by reliability (0-100) | Weighted composite scoring, tier classification |
| Stockout Analysis | Quantifies lost revenue from empty shelves | Root cause analysis, impact assessment |

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Generate 2 years of synthetic data (228K+ transactions)
python src/data_generator.py

# Launch interactive dashboard
streamlit run src/dashboard.py
```

Access dashboard at `http://localhost:8501`

## Project Structure

```
supply-chain-analytics/
├── data/                              # Generated CSV files (14 tables)
├── sql/
│   ├── 01_schema.sql                  # PostgreSQL database schema
│   └── 02_analytics_queries.sql       # Advanced SQL (CTEs, window functions)
├── src/
│   ├── data_generator.py              # Synthetic data with seasonal patterns
│   ├── analytics.py                   # Python analytics engine (4 classes)
│   └── dashboard.py                   # Streamlit dashboard (8 modules)
├── warehouse/                         # Enterprise scaling components
│   ├── snowflake_schema.sql           # Star schema for Snowflake
│   ├── tableau_export.py              # Tableau data extract generator
│   ├── tableau_extracts/              # Pre-built Tableau-ready CSVs
│   └── PLATFORM_EVALUATION.md         # Snowflake/Tableau evaluation
└── requirements.txt
```

## Data Generated

| Entity | Volume | Description |
|--------|--------|-------------|
| Products | 200 | SKUs with ABC distribution |
| Suppliers | 25 | With reliability factors |
| Warehouses | 5 | US distribution centers |
| Sales Orders | 34K+ | 2 years of customer orders |
| Sales Line Items | 228K+ | Individual transactions |
| Purchase Orders | 7K+ | Supplier orders |
| Inventory Records | 1,000 | Current stock levels |

## Core Formulas Implemented

**Reorder Point:**

```
ROP = (Avg Daily Demand × Lead Time) + Safety Stock
Safety Stock = Z × √(LT × σ_demand² + Demand² × σ_lead_time²)
```

**Economic Order Quantity:**

```
EOQ = √(2 × Annual Demand × Ordering Cost / Holding Cost per Unit)
```

**Supplier Reliability Score:**

```
Score = (On-Time Delivery % × 0.4) + (Fill Rate % × 0.4) + (Consistency × 0.2)
```

**Inventory Turnover:**

```
Turnover Ratio = Annual COGS / Average Inventory Value
Days of Supply = 365 / Turnover Ratio
```

## Sample SQL Analytics

```sql
-- ABC Classification with Window Functions
WITH product_revenue AS (
    SELECT 
        p.product_id,
        p.product_name,
        SUM(soi.quantity_ordered * soi.unit_price) AS total_revenue
    FROM products p
    JOIN sales_order_items soi ON p.product_id = soi.product_id
    GROUP BY p.product_id, p.product_name
),
ranked AS (
    SELECT *,
        SUM(total_revenue) OVER (ORDER BY total_revenue DESC) AS cumulative_revenue,
        SUM(total_revenue) OVER () AS grand_total
    FROM product_revenue
)
SELECT *,
    ROUND(cumulative_revenue / grand_total * 100, 2) AS cumulative_pct,
    CASE 
        WHEN cumulative_revenue / grand_total <= 0.80 THEN 'A'
        WHEN cumulative_revenue / grand_total <= 0.95 THEN 'B'
        ELSE 'C'
    END AS abc_class
FROM ranked
ORDER BY total_revenue DESC;
```

## Dashboard Sections

1. **Executive Summary** - KPIs, sales trends, warehouse distribution
2. **ABC Classification** - Revenue pie charts, top A-class products
3. **Inventory Turnover** - Movement classification, days of supply
4. **Supplier Performance** - Reliability matrix, tier rankings
5. **Stockout Analysis** - Root causes, lost revenue quantification
6. **Reorder Optimization** - Service level slider, ROP recommendations
7. **EOQ Analysis** - Configurable costs, optimal quantities
8. **Carrying Costs** - Cost breakdown by warehouse and category

## Technical Skills Demonstrated

- **SQL**: CTEs, window functions, aggregations, complex joins, views
- **Python**: Pandas, NumPy, OOP design, data pipelines
- **Analytics**: ABC analysis, EOQ, safety stock, demand forecasting
- **Visualization**: Streamlit, Plotly, interactive dashboards
- **Database Design**: Normalized schema, referential integrity, indexing
- **Operations Research**: Inventory optimization, service level targeting

## Results

- Analyzed $51M inventory portfolio across 5 warehouses
- Identified 30 A-class products generating 80% of $2.8B revenue
- Calculated $10.3M annual carrying costs with component breakdown
- Scored 25 suppliers on composite reliability metrics
- Generated actionable reorder point recommendations for 200 SKUs

---

## Enterprise Scaling: Snowflake & Tableau

This project includes evaluation and implementation artifacts for scaling to enterprise analytics infrastructure.

### Snowflake Data Warehouse

The `warehouse/snowflake_schema.sql` file contains a production-ready **star schema** design:

| Component | Description |
|-----------|-------------|
| Dimension Tables | DIM_DATE, DIM_PRODUCT, DIM_SUPPLIER, DIM_WAREHOUSE, DIM_CUSTOMER |
| Fact Tables | FACT_SALES, FACT_INVENTORY_DAILY, FACT_PURCHASES, FACT_STOCKOUTS |
| Aggregate Tables | AGG_SALES_MONTHLY, AGG_SUPPLIER_PERFORMANCE, AGG_ABC_CLASSIFICATION |
| Tableau Views | VW_SALES_DASHBOARD, VW_INVENTORY_HEALTH, VW_SUPPLIER_SCORECARD |

**Snowflake-specific optimizations:**

- Clustering keys on date and product for query performance
- Search optimization for common filter columns
- Stored procedures for ETL and ABC classification refresh

### Tableau Data Extracts

Generate Tableau-ready denormalized exports:

```bash
cd warehouse
python tableau_export.py --data-dir ../data --output-dir ./tableau_extracts
```

**Exports generated:**

| Extract | Rows | Grain | Use Case |
|---------|------|-------|----------|
| tableau_sales_extract.csv | 228K | Line item | Sales dashboards, revenue analysis |
| tableau_inventory_extract.csv | 1K | Product-warehouse | Inventory health, reorder alerts |
| tableau_supplier_extract.csv | 25 | Supplier | Supplier scorecard, performance matrix |
| tableau_timeseries_extract.csv | 35K | Daily | Trend analysis, forecasting |

### Platform Evaluation

See `warehouse/PLATFORM_EVALUATION.md` for:

- Detailed comparison of Snowflake vs Redshift vs BigQuery
- Tableau vs Power BI vs Looker evaluation matrix
- Implementation roadmap and cost projections
- Risk assessment and success metrics

---

## Author

**Krishna Somisetty**  
[LinkedIn](https://linkedin.com/in/krishnasomisetty) | krishsomi003@gmail.com
