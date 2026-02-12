-- ============================================================================
-- SNOWFLAKE DATA WAREHOUSE SCHEMA
-- Supply Chain Analytics - Star Schema Design
-- ============================================================================
-- This schema transforms the operational PostgreSQL database into an analytics-
-- optimized star schema for Snowflake, enabling efficient BI reporting and
-- dashboard performance at scale.
-- ============================================================================

-- Create database and schema
CREATE DATABASE IF NOT EXISTS SUPPLY_CHAIN_DW;
USE DATABASE SUPPLY_CHAIN_DW;

CREATE SCHEMA IF NOT EXISTS ANALYTICS;
USE SCHEMA ANALYTICS;

-- ============================================================================
-- DIMENSION TABLES
-- ============================================================================

-- Date Dimension (for time-based analysis)
CREATE OR REPLACE TABLE DIM_DATE (
    date_key            INT PRIMARY KEY,
    full_date           DATE NOT NULL,
    day_of_week         INT,
    day_name            VARCHAR(10),
    day_of_month        INT,
    day_of_year         INT,
    week_of_year        INT,
    month_num           INT,
    month_name          VARCHAR(10),
    quarter             INT,
    year                INT,
    is_weekend          BOOLEAN,
    is_holiday          BOOLEAN,
    fiscal_quarter      INT,
    fiscal_year         INT
);

-- Product Dimension
CREATE OR REPLACE TABLE DIM_PRODUCT (
    product_key         INT PRIMARY KEY AUTOINCREMENT,
    product_id          INT NOT NULL,
    sku                 VARCHAR(50),
    product_name        VARCHAR(200),
    category_id         INT,
    category_name       VARCHAR(100),
    unit_cost           DECIMAL(12,2),
    unit_price          DECIMAL(12,2),
    profit_margin       DECIMAL(5,2),
    abc_class           CHAR(1),
    lead_time_days      INT,
    reorder_point       INT,
    safety_stock        INT,
    is_active           BOOLEAN,
    effective_date      DATE,
    expiration_date     DATE,
    is_current          BOOLEAN
);

-- Supplier Dimension
CREATE OR REPLACE TABLE DIM_SUPPLIER (
    supplier_key        INT PRIMARY KEY AUTOINCREMENT,
    supplier_id         INT NOT NULL,
    supplier_code       VARCHAR(20),
    supplier_name       VARCHAR(200),
    city                VARCHAR(100),
    state               VARCHAR(50),
    country             VARCHAR(50),
    payment_terms_days  INT,
    quality_rating      DECIMAL(3,2),
    reliability_score   DECIMAL(5,2),
    reliability_tier    VARCHAR(20),
    is_preferred        BOOLEAN,
    is_active           BOOLEAN,
    effective_date      DATE,
    expiration_date     DATE,
    is_current          BOOLEAN
);

-- Warehouse Dimension
CREATE OR REPLACE TABLE DIM_WAREHOUSE (
    warehouse_key       INT PRIMARY KEY AUTOINCREMENT,
    warehouse_id        INT NOT NULL,
    warehouse_code      VARCHAR(20),
    warehouse_name      VARCHAR(200),
    city                VARCHAR(100),
    state               VARCHAR(50),
    region              VARCHAR(50),
    latitude            DECIMAL(10,6),
    longitude           DECIMAL(10,6),
    capacity_units      INT,
    operating_cost_per_unit DECIMAL(10,2),
    is_active           BOOLEAN
);

-- Customer Dimension
CREATE OR REPLACE TABLE DIM_CUSTOMER (
    customer_key        INT PRIMARY KEY AUTOINCREMENT,
    customer_id         INT NOT NULL,
    customer_code       VARCHAR(20),
    customer_name       VARCHAR(200),
    customer_type       VARCHAR(50),
    city                VARCHAR(100),
    state               VARCHAR(50),
    country             VARCHAR(50),
    credit_limit        DECIMAL(12,2),
    is_active           BOOLEAN,
    effective_date      DATE,
    expiration_date     DATE,
    is_current          BOOLEAN
);

-- ============================================================================
-- FACT TABLES
-- ============================================================================

-- Sales Fact Table (grain: one row per sales order line item)
CREATE OR REPLACE TABLE FACT_SALES (
    sales_fact_key      INT PRIMARY KEY AUTOINCREMENT,
    date_key            INT NOT NULL REFERENCES DIM_DATE(date_key),
    product_key         INT NOT NULL REFERENCES DIM_PRODUCT(product_key),
    customer_key        INT NOT NULL REFERENCES DIM_CUSTOMER(customer_key),
    warehouse_key       INT NOT NULL REFERENCES DIM_WAREHOUSE(warehouse_key),
    
    -- Degenerate dimensions
    sales_order_number  VARCHAR(50),
    line_item_number    INT,
    
    -- Measures
    quantity_ordered    INT,
    quantity_shipped    INT,
    unit_price          DECIMAL(12,2),
    discount_percent    DECIMAL(5,2),
    gross_amount        DECIMAL(12,2),
    discount_amount     DECIMAL(12,2),
    net_amount          DECIMAL(12,2),
    cost_amount         DECIMAL(12,2),
    profit_amount       DECIMAL(12,2),
    
    -- Shipping metrics
    days_to_ship        INT,
    is_on_time          BOOLEAN
);

-- Inventory Fact Table (grain: daily snapshot per product per warehouse)
CREATE OR REPLACE TABLE FACT_INVENTORY_DAILY (
    inventory_fact_key  INT PRIMARY KEY AUTOINCREMENT,
    date_key            INT NOT NULL REFERENCES DIM_DATE(date_key),
    product_key         INT NOT NULL REFERENCES DIM_PRODUCT(product_key),
    warehouse_key       INT NOT NULL REFERENCES DIM_WAREHOUSE(warehouse_key),
    
    -- Measures
    quantity_on_hand    INT,
    quantity_reserved   INT,
    quantity_available  INT,
    reorder_point       INT,
    inventory_value     DECIMAL(14,2),
    days_of_supply      DECIMAL(10,2),
    
    -- Flags
    is_below_reorder    BOOLEAN,
    is_stockout         BOOLEAN,
    is_overstock        BOOLEAN
);

-- Purchase Order Fact Table (grain: one row per PO line item)
CREATE OR REPLACE TABLE FACT_PURCHASES (
    purchase_fact_key   INT PRIMARY KEY AUTOINCREMENT,
    order_date_key      INT NOT NULL REFERENCES DIM_DATE(date_key),
    delivery_date_key   INT REFERENCES DIM_DATE(date_key),
    product_key         INT NOT NULL REFERENCES DIM_PRODUCT(product_key),
    supplier_key        INT NOT NULL REFERENCES DIM_SUPPLIER(supplier_key),
    warehouse_key       INT NOT NULL REFERENCES DIM_WAREHOUSE(warehouse_key),
    
    -- Degenerate dimensions
    purchase_order_number VARCHAR(50),
    line_item_number    INT,
    
    -- Measures
    quantity_ordered    INT,
    quantity_received   INT,
    unit_cost           DECIMAL(12,2),
    total_cost          DECIMAL(14,2),
    
    -- Lead time metrics
    expected_lead_days  INT,
    actual_lead_days    INT,
    lead_time_variance  INT,
    
    -- Flags
    is_on_time          BOOLEAN,
    is_complete         BOOLEAN
);

-- Stockout Fact Table (grain: one row per stockout event)
CREATE OR REPLACE TABLE FACT_STOCKOUTS (
    stockout_fact_key   INT PRIMARY KEY AUTOINCREMENT,
    start_date_key      INT NOT NULL REFERENCES DIM_DATE(date_key),
    end_date_key        INT REFERENCES DIM_DATE(date_key),
    product_key         INT NOT NULL REFERENCES DIM_PRODUCT(product_key),
    warehouse_key       INT NOT NULL REFERENCES DIM_WAREHOUSE(warehouse_key),
    
    -- Measures
    duration_days       INT,
    estimated_lost_units INT,
    estimated_lost_revenue DECIMAL(14,2),
    
    -- Root cause
    root_cause          VARCHAR(100)
);

-- ============================================================================
-- AGGREGATE TABLES (for dashboard performance)
-- ============================================================================

-- Monthly Sales Summary
CREATE OR REPLACE TABLE AGG_SALES_MONTHLY (
    year                INT,
    month               INT,
    product_key         INT REFERENCES DIM_PRODUCT(product_key),
    warehouse_key       INT REFERENCES DIM_WAREHOUSE(warehouse_key),
    
    total_orders        INT,
    total_quantity      INT,
    gross_revenue       DECIMAL(14,2),
    net_revenue         DECIMAL(14,2),
    total_cost          DECIMAL(14,2),
    total_profit        DECIMAL(14,2),
    avg_order_value     DECIMAL(12,2),
    
    PRIMARY KEY (year, month, product_key, warehouse_key)
);

-- Supplier Performance Summary
CREATE OR REPLACE TABLE AGG_SUPPLIER_PERFORMANCE (
    year                INT,
    quarter             INT,
    supplier_key        INT REFERENCES DIM_SUPPLIER(supplier_key),
    
    total_orders        INT,
    total_units_ordered INT,
    total_units_received INT,
    total_spend         DECIMAL(14,2),
    
    on_time_deliveries  INT,
    late_deliveries     INT,
    on_time_rate        DECIMAL(5,2),
    fill_rate           DECIMAL(5,2),
    avg_lead_time       DECIMAL(10,2),
    lead_time_std_dev   DECIMAL(10,2),
    
    PRIMARY KEY (year, quarter, supplier_key)
);

-- ABC Classification Summary (refreshed periodically)
CREATE OR REPLACE TABLE AGG_ABC_CLASSIFICATION (
    analysis_date       DATE,
    product_key         INT REFERENCES DIM_PRODUCT(product_key),
    
    total_revenue       DECIMAL(14,2),
    revenue_rank        INT,
    cumulative_revenue  DECIMAL(14,2),
    cumulative_pct      DECIMAL(5,2),
    abc_class           CHAR(1),
    
    PRIMARY KEY (analysis_date, product_key)
);

-- ============================================================================
-- VIEWS FOR TABLEAU INTEGRATION
-- ============================================================================

-- Sales Dashboard View
CREATE OR REPLACE VIEW VW_SALES_DASHBOARD AS
SELECT 
    d.full_date,
    d.year,
    d.quarter,
    d.month_name,
    d.week_of_year,
    d.day_name,
    d.is_weekend,
    
    p.sku,
    p.product_name,
    p.category_name,
    p.abc_class,
    
    c.customer_name,
    c.customer_type,
    c.state AS customer_state,
    
    w.warehouse_name,
    w.region,
    
    f.quantity_ordered,
    f.quantity_shipped,
    f.net_amount,
    f.profit_amount,
    f.is_on_time
FROM FACT_SALES f
JOIN DIM_DATE d ON f.date_key = d.date_key
JOIN DIM_PRODUCT p ON f.product_key = p.product_key
JOIN DIM_CUSTOMER c ON f.customer_key = c.customer_key
JOIN DIM_WAREHOUSE w ON f.warehouse_key = w.warehouse_key
WHERE p.is_current = TRUE 
  AND c.is_current = TRUE;

-- Inventory Health View
CREATE OR REPLACE VIEW VW_INVENTORY_HEALTH AS
SELECT 
    d.full_date,
    d.year,
    d.month_name,
    
    p.sku,
    p.product_name,
    p.category_name,
    p.abc_class,
    
    w.warehouse_name,
    w.region,
    
    f.quantity_on_hand,
    f.quantity_available,
    f.reorder_point,
    f.inventory_value,
    f.days_of_supply,
    f.is_below_reorder,
    f.is_stockout,
    
    CASE 
        WHEN f.is_stockout THEN 'Out of Stock'
        WHEN f.is_below_reorder THEN 'Below Reorder Point'
        WHEN f.days_of_supply > 90 THEN 'Overstock'
        ELSE 'Healthy'
    END AS inventory_status
FROM FACT_INVENTORY_DAILY f
JOIN DIM_DATE d ON f.date_key = d.date_key
JOIN DIM_PRODUCT p ON f.product_key = p.product_key
JOIN DIM_WAREHOUSE w ON f.warehouse_key = w.warehouse_key
WHERE p.is_current = TRUE;

-- Supplier Scorecard View
CREATE OR REPLACE VIEW VW_SUPPLIER_SCORECARD AS
SELECT 
    s.supplier_name,
    s.reliability_tier,
    s.quality_rating,
    s.is_preferred,
    
    a.year,
    a.quarter,
    a.total_orders,
    a.total_spend,
    a.on_time_rate,
    a.fill_rate,
    a.avg_lead_time,
    
    -- Performance indicators
    CASE 
        WHEN a.on_time_rate >= 95 AND a.fill_rate >= 98 THEN 'Excellent'
        WHEN a.on_time_rate >= 90 AND a.fill_rate >= 95 THEN 'Good'
        WHEN a.on_time_rate >= 80 AND a.fill_rate >= 90 THEN 'Fair'
        ELSE 'Needs Improvement'
    END AS performance_rating
FROM AGG_SUPPLIER_PERFORMANCE a
JOIN DIM_SUPPLIER s ON a.supplier_key = s.supplier_key
WHERE s.is_current = TRUE;

-- ============================================================================
-- ETL STORED PROCEDURES
-- ============================================================================

-- Procedure to populate date dimension
CREATE OR REPLACE PROCEDURE SP_POPULATE_DATE_DIM(start_date DATE, end_date DATE)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO DIM_DATE
    SELECT 
        TO_NUMBER(TO_CHAR(date_val, 'YYYYMMDD')) AS date_key,
        date_val AS full_date,
        DAYOFWEEK(date_val) AS day_of_week,
        DAYNAME(date_val) AS day_name,
        DAY(date_val) AS day_of_month,
        DAYOFYEAR(date_val) AS day_of_year,
        WEEKOFYEAR(date_val) AS week_of_year,
        MONTH(date_val) AS month_num,
        MONTHNAME(date_val) AS month_name,
        QUARTER(date_val) AS quarter,
        YEAR(date_val) AS year,
        CASE WHEN DAYOFWEEK(date_val) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
        FALSE AS is_holiday,
        QUARTER(date_val) AS fiscal_quarter,
        YEAR(date_val) AS fiscal_year
    FROM (
        SELECT DATEADD(day, SEQ4(), :start_date) AS date_val
        FROM TABLE(GENERATOR(ROWCOUNT => DATEDIFF(day, :start_date, :end_date) + 1))
    );
    RETURN 'Date dimension populated successfully';
END;
$$;

-- Procedure to refresh ABC classification
CREATE OR REPLACE PROCEDURE SP_REFRESH_ABC_CLASSIFICATION()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO AGG_ABC_CLASSIFICATION
    WITH revenue_calc AS (
        SELECT 
            product_key,
            SUM(net_amount) AS total_revenue
        FROM FACT_SALES
        WHERE date_key >= TO_NUMBER(TO_CHAR(DATEADD(year, -1, CURRENT_DATE()), 'YYYYMMDD'))
        GROUP BY product_key
    ),
    ranked AS (
        SELECT 
            product_key,
            total_revenue,
            ROW_NUMBER() OVER (ORDER BY total_revenue DESC) AS revenue_rank,
            SUM(total_revenue) OVER (ORDER BY total_revenue DESC) AS cumulative_revenue,
            SUM(total_revenue) OVER () AS grand_total
        FROM revenue_calc
    )
    SELECT 
        CURRENT_DATE() AS analysis_date,
        product_key,
        total_revenue,
        revenue_rank,
        cumulative_revenue,
        ROUND(cumulative_revenue / grand_total * 100, 2) AS cumulative_pct,
        CASE 
            WHEN cumulative_revenue / grand_total <= 0.80 THEN 'A'
            WHEN cumulative_revenue / grand_total <= 0.95 THEN 'B'
            ELSE 'C'
        END AS abc_class
    FROM ranked;
    
    RETURN 'ABC classification refreshed successfully';
END;
$$;

-- ============================================================================
-- SNOWFLAKE-SPECIFIC OPTIMIZATIONS
-- ============================================================================

-- Clustering keys for large fact tables
ALTER TABLE FACT_SALES CLUSTER BY (date_key, product_key);
ALTER TABLE FACT_INVENTORY_DAILY CLUSTER BY (date_key, warehouse_key);
ALTER TABLE FACT_PURCHASES CLUSTER BY (order_date_key, supplier_key);

-- Search optimization for common filter columns
ALTER TABLE FACT_SALES ADD SEARCH OPTIMIZATION ON EQUALITY(product_key, customer_key);
ALTER TABLE DIM_PRODUCT ADD SEARCH OPTIMIZATION ON EQUALITY(sku, abc_class);

-- ============================================================================
-- SAMPLE QUERIES FOR VALIDATION
-- ============================================================================

-- Query 1: Monthly revenue trend by ABC class
/*
SELECT 
    d.year,
    d.month_name,
    p.abc_class,
    SUM(f.net_amount) AS total_revenue,
    COUNT(DISTINCT f.sales_order_number) AS order_count
FROM FACT_SALES f
JOIN DIM_DATE d ON f.date_key = d.date_key
JOIN DIM_PRODUCT p ON f.product_key = p.product_key
WHERE p.is_current = TRUE
GROUP BY d.year, d.month_name, d.month_num, p.abc_class
ORDER BY d.year, d.month_num, p.abc_class;
*/

-- Query 2: Supplier performance comparison
/*
SELECT 
    s.supplier_name,
    s.reliability_tier,
    COUNT(*) AS total_orders,
    ROUND(AVG(f.actual_lead_days), 1) AS avg_lead_time,
    ROUND(SUM(CASE WHEN f.is_on_time THEN 1 ELSE 0 END)::FLOAT / COUNT(*) * 100, 1) AS on_time_pct,
    ROUND(SUM(f.quantity_received)::FLOAT / SUM(f.quantity_ordered) * 100, 1) AS fill_rate_pct
FROM FACT_PURCHASES f
JOIN DIM_SUPPLIER s ON f.supplier_key = s.supplier_key
WHERE s.is_current = TRUE
GROUP BY s.supplier_name, s.reliability_tier
ORDER BY on_time_pct DESC;
*/
