-- ============================================================================
-- SUPPLY CHAIN ANALYTICS SQL QUERIES
-- Author: Krishna Somisetty
-- Description: Advanced SQL analytics for inventory optimization and supply chain management
-- ============================================================================

-- ============================================================================
-- 1. ABC CLASSIFICATION ANALYSIS
-- Classifies products based on cumulative revenue contribution
-- ============================================================================

WITH product_revenue AS (
    SELECT 
        p.product_id,
        p.sku,
        p.product_name,
        pc.category_name,
        SUM(soi.line_total) AS total_revenue,
        SUM(soi.quantity_ordered) AS total_units_sold
    FROM products p
    JOIN sales_order_items soi ON p.product_id = soi.product_id
    JOIN sales_orders so ON soi.so_id = so.so_id
    LEFT JOIN product_categories pc ON p.category_id = pc.category_id
    WHERE so.status NOT IN ('Cancelled')
        AND so.order_date >= CURRENT_DATE - INTERVAL '12 months'
    GROUP BY p.product_id, p.sku, p.product_name, pc.category_name
),
ranked_products AS (
    SELECT 
        *,
        SUM(total_revenue) OVER () AS grand_total_revenue,
        SUM(total_revenue) OVER (ORDER BY total_revenue DESC) AS cumulative_revenue,
        ROW_NUMBER() OVER (ORDER BY total_revenue DESC) AS revenue_rank
    FROM product_revenue
),
abc_classified AS (
    SELECT 
        *,
        (cumulative_revenue / grand_total_revenue * 100) AS cumulative_percentage,
        CASE 
            WHEN (cumulative_revenue / grand_total_revenue * 100) <= 80 THEN 'A'
            WHEN (cumulative_revenue / grand_total_revenue * 100) <= 95 THEN 'B'
            ELSE 'C'
        END AS abc_class
    FROM ranked_products
)
SELECT 
    abc_class,
    COUNT(*) AS product_count,
    ROUND(COUNT(*)::DECIMAL / SUM(COUNT(*)) OVER () * 100, 2) AS pct_of_products,
    SUM(total_revenue) AS class_revenue,
    ROUND(SUM(total_revenue) / SUM(SUM(total_revenue)) OVER () * 100, 2) AS pct_of_revenue,
    ROUND(AVG(total_units_sold), 0) AS avg_units_per_product
FROM abc_classified
GROUP BY abc_class
ORDER BY abc_class;

-- Detailed ABC by Product
SELECT 
    product_id,
    sku,
    product_name,
    category_name,
    total_revenue,
    total_units_sold,
    revenue_rank,
    ROUND(cumulative_percentage, 2) AS cumulative_pct,
    abc_class
FROM abc_classified
ORDER BY revenue_rank;


-- ============================================================================
-- 2. INVENTORY TURNOVER ANALYSIS
-- Measures how efficiently inventory is being sold and replaced
-- ============================================================================

WITH inventory_metrics AS (
    SELECT 
        p.product_id,
        p.sku,
        p.product_name,
        w.warehouse_code,
        i.quantity_on_hand,
        p.unit_cost,
        (i.quantity_on_hand * p.unit_cost) AS current_inventory_value,
        -- Calculate COGS for the period
        COALESCE(SUM(soi.quantity_ordered * p.unit_cost), 0) AS cogs_12m,
        -- Calculate average inventory (simplified: current + beginning / 2)
        (i.quantity_on_hand * p.unit_cost) AS avg_inventory_value,
        COALESCE(SUM(soi.quantity_ordered), 0) AS units_sold_12m
    FROM products p
    JOIN inventory i ON p.product_id = i.product_id
    JOIN warehouses w ON i.warehouse_id = w.warehouse_id
    LEFT JOIN sales_order_items soi ON p.product_id = soi.product_id
    LEFT JOIN sales_orders so ON soi.so_id = so.so_id
        AND so.status NOT IN ('Cancelled')
        AND so.order_date >= CURRENT_DATE - INTERVAL '12 months'
    GROUP BY p.product_id, p.sku, p.product_name, w.warehouse_code, 
             i.quantity_on_hand, p.unit_cost
)
SELECT 
    product_id,
    sku,
    product_name,
    warehouse_code,
    quantity_on_hand,
    current_inventory_value,
    cogs_12m,
    units_sold_12m,
    -- Inventory Turnover Ratio
    CASE 
        WHEN avg_inventory_value > 0 THEN ROUND(cogs_12m / avg_inventory_value, 2)
        ELSE 0 
    END AS inventory_turnover_ratio,
    -- Days of Inventory on Hand
    CASE 
        WHEN cogs_12m > 0 THEN ROUND(365 * avg_inventory_value / cogs_12m, 0)
        ELSE NULL 
    END AS days_inventory_on_hand,
    -- Turnover Classification
    CASE 
        WHEN cogs_12m / NULLIF(avg_inventory_value, 0) >= 12 THEN 'Fast Moving'
        WHEN cogs_12m / NULLIF(avg_inventory_value, 0) >= 4 THEN 'Normal'
        WHEN cogs_12m / NULLIF(avg_inventory_value, 0) >= 1 THEN 'Slow Moving'
        ELSE 'Dead Stock'
    END AS turnover_category
FROM inventory_metrics
WHERE quantity_on_hand > 0
ORDER BY 
    CASE WHEN avg_inventory_value > 0 THEN cogs_12m / avg_inventory_value ELSE 0 END DESC;


-- ============================================================================
-- 3. SUPPLIER PERFORMANCE ANALYSIS
-- Comprehensive supplier reliability scoring
-- ============================================================================

WITH supplier_deliveries AS (
    SELECT 
        s.supplier_id,
        s.supplier_code,
        s.supplier_name,
        po.po_id,
        po.expected_delivery_date,
        po.actual_delivery_date,
        po.total_amount,
        -- Delivery variance in days
        CASE 
            WHEN po.actual_delivery_date IS NOT NULL 
            THEN po.actual_delivery_date - po.expected_delivery_date 
        END AS delivery_variance_days,
        -- On-time flag
        CASE 
            WHEN po.actual_delivery_date <= po.expected_delivery_date THEN 1 
            ELSE 0 
        END AS is_on_time
    FROM suppliers s
    JOIN purchase_orders po ON s.supplier_id = po.supplier_id
    WHERE po.status = 'Delivered'
        AND po.order_date >= CURRENT_DATE - INTERVAL '12 months'
),
supplier_quality AS (
    SELECT 
        ps.supplier_id,
        COUNT(poi.po_item_id) AS total_line_items,
        SUM(poi.quantity_ordered) AS total_qty_ordered,
        SUM(poi.quantity_received) AS total_qty_received,
        -- Quality rate (received vs ordered)
        ROUND(SUM(poi.quantity_received)::DECIMAL / NULLIF(SUM(poi.quantity_ordered), 0) * 100, 2) AS fill_rate
    FROM product_suppliers ps
    JOIN purchase_order_items poi ON ps.product_id = poi.product_id
    JOIN purchase_orders po ON poi.po_id = po.po_id AND ps.supplier_id = po.supplier_id
    WHERE po.status = 'Delivered'
    GROUP BY ps.supplier_id
)
SELECT 
    sd.supplier_id,
    sd.supplier_code,
    sd.supplier_name,
    COUNT(sd.po_id) AS total_orders,
    SUM(sd.total_amount) AS total_purchase_value,
    -- Delivery Metrics
    ROUND(AVG(sd.delivery_variance_days), 1) AS avg_delivery_variance_days,
    ROUND(STDDEV(sd.delivery_variance_days), 1) AS delivery_variance_stddev,
    ROUND(SUM(sd.is_on_time)::DECIMAL / COUNT(*) * 100, 2) AS on_time_delivery_pct,
    -- Quality Metrics
    COALESCE(sq.fill_rate, 0) AS fill_rate_pct,
    -- Composite Reliability Score (0-100)
    ROUND(
        (SUM(sd.is_on_time)::DECIMAL / COUNT(*) * 40) +  -- 40% weight for on-time
        (COALESCE(sq.fill_rate, 0) * 0.4) +               -- 40% weight for fill rate
        (CASE 
            WHEN ABS(AVG(sd.delivery_variance_days)) <= 1 THEN 20
            WHEN ABS(AVG(sd.delivery_variance_days)) <= 3 THEN 15
            WHEN ABS(AVG(sd.delivery_variance_days)) <= 7 THEN 10
            ELSE 5 
        END)                                              -- 20% weight for consistency
    , 1) AS reliability_score,
    -- Reliability Tier
    CASE 
        WHEN (SUM(sd.is_on_time)::DECIMAL / COUNT(*)) >= 0.95 
            AND COALESCE(sq.fill_rate, 0) >= 98 THEN 'Platinum'
        WHEN (SUM(sd.is_on_time)::DECIMAL / COUNT(*)) >= 0.90 
            AND COALESCE(sq.fill_rate, 0) >= 95 THEN 'Gold'
        WHEN (SUM(sd.is_on_time)::DECIMAL / COUNT(*)) >= 0.80 
            AND COALESCE(sq.fill_rate, 0) >= 90 THEN 'Silver'
        ELSE 'Bronze'
    END AS supplier_tier
FROM supplier_deliveries sd
LEFT JOIN supplier_quality sq ON sd.supplier_id = sq.supplier_id
GROUP BY sd.supplier_id, sd.supplier_code, sd.supplier_name, sq.fill_rate
ORDER BY reliability_score DESC;


-- ============================================================================
-- 4. STOCKOUT ANALYSIS
-- Identifies patterns and impact of stockouts
-- ============================================================================

WITH stockout_metrics AS (
    SELECT 
        se.warehouse_id,
        w.warehouse_code,
        se.product_id,
        p.sku,
        p.product_name,
        pc.category_name,
        COUNT(*) AS stockout_count,
        SUM(se.demand_during_stockout) AS total_lost_demand,
        SUM(se.lost_sales_amount) AS total_lost_revenue,
        AVG(EXTRACT(EPOCH FROM (COALESCE(se.stockout_end_date, CURRENT_TIMESTAMP) - se.stockout_start_date)) / 86400) AS avg_stockout_days,
        MAX(se.stockout_start_date) AS last_stockout_date
    FROM stockout_events se
    JOIN warehouses w ON se.warehouse_id = w.warehouse_id
    JOIN products p ON se.product_id = p.product_id
    LEFT JOIN product_categories pc ON p.category_id = pc.category_id
    WHERE se.stockout_start_date >= CURRENT_DATE - INTERVAL '12 months'
    GROUP BY se.warehouse_id, w.warehouse_code, se.product_id, p.sku, p.product_name, pc.category_name
)
SELECT 
    warehouse_code,
    sku,
    product_name,
    category_name,
    stockout_count,
    ROUND(avg_stockout_days, 1) AS avg_stockout_duration_days,
    total_lost_demand AS lost_units,
    ROUND(total_lost_revenue, 2) AS lost_revenue,
    last_stockout_date,
    -- Stockout severity score
    ROUND(
        (stockout_count * 30) + 
        (avg_stockout_days * 10) + 
        (COALESCE(total_lost_revenue, 0) / 100)
    , 0) AS severity_score
FROM stockout_metrics
ORDER BY severity_score DESC
LIMIT 50;

-- Stockout Frequency by Day of Week
SELECT 
    TO_CHAR(se.stockout_start_date, 'Day') AS day_of_week,
    EXTRACT(DOW FROM se.stockout_start_date) AS dow_num,
    COUNT(*) AS stockout_count,
    SUM(se.lost_sales_amount) AS lost_revenue
FROM stockout_events se
WHERE se.stockout_start_date >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY TO_CHAR(se.stockout_start_date, 'Day'), EXTRACT(DOW FROM se.stockout_start_date)
ORDER BY dow_num;


-- ============================================================================
-- 5. REORDER POINT CALCULATION
-- Dynamic reorder point based on demand variability and lead time
-- ============================================================================

WITH daily_demand AS (
    SELECT 
        soi.product_id,
        so.warehouse_id,
        so.order_date,
        SUM(soi.quantity_ordered) AS daily_qty
    FROM sales_order_items soi
    JOIN sales_orders so ON soi.so_id = so.so_id
    WHERE so.status NOT IN ('Cancelled')
        AND so.order_date >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY soi.product_id, so.warehouse_id, so.order_date
),
demand_stats AS (
    SELECT 
        product_id,
        warehouse_id,
        AVG(daily_qty) AS avg_daily_demand,
        STDDEV(daily_qty) AS stddev_daily_demand,
        COUNT(*) AS days_with_demand
    FROM daily_demand
    GROUP BY product_id, warehouse_id
),
lead_time_stats AS (
    SELECT 
        ps.product_id,
        AVG(po.actual_delivery_date - po.order_date) AS avg_lead_time_days,
        STDDEV(po.actual_delivery_date - po.order_date) AS stddev_lead_time_days
    FROM product_suppliers ps
    JOIN purchase_orders po ON ps.supplier_id = po.supplier_id
    JOIN purchase_order_items poi ON po.po_id = poi.po_id AND ps.product_id = poi.product_id
    WHERE po.actual_delivery_date IS NOT NULL
        AND po.order_date >= CURRENT_DATE - INTERVAL '12 months'
    GROUP BY ps.product_id
)
SELECT 
    p.product_id,
    p.sku,
    p.product_name,
    w.warehouse_code,
    i.quantity_on_hand,
    ROUND(ds.avg_daily_demand, 2) AS avg_daily_demand,
    ROUND(COALESCE(lts.avg_lead_time_days, p.lead_time_days), 0) AS lead_time_days,
    -- Safety Stock (z-score of 1.65 for 95% service level)
    ROUND(1.65 * SQRT(
        COALESCE(lts.avg_lead_time_days, p.lead_time_days) * POWER(COALESCE(ds.stddev_daily_demand, ds.avg_daily_demand * 0.3), 2) +
        POWER(ds.avg_daily_demand, 2) * POWER(COALESCE(lts.stddev_lead_time_days, lts.avg_lead_time_days * 0.2), 2)
    ), 0) AS safety_stock,
    -- Reorder Point = (Avg Daily Demand * Lead Time) + Safety Stock
    ROUND(
        ds.avg_daily_demand * COALESCE(lts.avg_lead_time_days, p.lead_time_days) +
        1.65 * SQRT(
            COALESCE(lts.avg_lead_time_days, p.lead_time_days) * POWER(COALESCE(ds.stddev_daily_demand, ds.avg_daily_demand * 0.3), 2) +
            POWER(ds.avg_daily_demand, 2) * POWER(COALESCE(lts.stddev_lead_time_days, lts.avg_lead_time_days * 0.2), 2)
        )
    , 0) AS calculated_reorder_point,
    i.reorder_point AS current_reorder_point,
    -- Recommended adjustment
    CASE 
        WHEN ROUND(
            ds.avg_daily_demand * COALESCE(lts.avg_lead_time_days, p.lead_time_days) +
            1.65 * SQRT(
                COALESCE(lts.avg_lead_time_days, p.lead_time_days) * POWER(COALESCE(ds.stddev_daily_demand, ds.avg_daily_demand * 0.3), 2) +
                POWER(ds.avg_daily_demand, 2) * POWER(COALESCE(lts.stddev_lead_time_days, lts.avg_lead_time_days * 0.2), 2)
            )
        , 0) > COALESCE(i.reorder_point, 0) * 1.2 THEN 'Increase ROP'
        WHEN ROUND(
            ds.avg_daily_demand * COALESCE(lts.avg_lead_time_days, p.lead_time_days) +
            1.65 * SQRT(
                COALESCE(lts.avg_lead_time_days, p.lead_time_days) * POWER(COALESCE(ds.stddev_daily_demand, ds.avg_daily_demand * 0.3), 2) +
                POWER(ds.avg_daily_demand, 2) * POWER(COALESCE(lts.stddev_lead_time_days, lts.avg_lead_time_days * 0.2), 2)
            )
        , 0) < COALESCE(i.reorder_point, 0) * 0.8 THEN 'Decrease ROP'
        ELSE 'Optimal'
    END AS recommendation
FROM products p
JOIN inventory i ON p.product_id = i.product_id
JOIN warehouses w ON i.warehouse_id = w.warehouse_id
LEFT JOIN demand_stats ds ON p.product_id = ds.product_id AND i.warehouse_id = ds.warehouse_id
LEFT JOIN lead_time_stats lts ON p.product_id = lts.product_id
WHERE ds.avg_daily_demand IS NOT NULL
ORDER BY ds.avg_daily_demand DESC;


-- ============================================================================
-- 6. ECONOMIC ORDER QUANTITY (EOQ) CALCULATION
-- Optimal order quantity to minimize total inventory costs
-- ============================================================================

WITH annual_demand AS (
    SELECT 
        soi.product_id,
        SUM(soi.quantity_ordered) AS total_demand,
        COUNT(DISTINCT so.order_date) AS demand_days
    FROM sales_order_items soi
    JOIN sales_orders so ON soi.so_id = so.so_id
    WHERE so.status NOT IN ('Cancelled')
        AND so.order_date >= CURRENT_DATE - INTERVAL '12 months'
    GROUP BY soi.product_id
),
cost_params AS (
    SELECT 
        p.product_id,
        p.sku,
        p.product_name,
        p.unit_cost,
        -- Ordering cost (fixed cost per order)
        50.00 AS ordering_cost,  -- Assumed $50 per order
        -- Holding cost rate (typically 20-30% of unit cost per year)
        0.25 AS holding_cost_rate
    FROM products p
)
SELECT 
    cp.product_id,
    cp.sku,
    cp.product_name,
    cp.unit_cost,
    COALESCE(ad.total_demand, 0) AS annual_demand,
    cp.ordering_cost,
    ROUND(cp.unit_cost * cp.holding_cost_rate, 2) AS annual_holding_cost_per_unit,
    -- EOQ Formula: sqrt(2 * D * S / H)
    -- D = Annual Demand, S = Ordering Cost, H = Holding Cost per Unit
    CASE 
        WHEN ad.total_demand > 0 THEN
            ROUND(SQRT(
                2 * ad.total_demand * cp.ordering_cost / 
                (cp.unit_cost * cp.holding_cost_rate)
            ), 0)
        ELSE 0 
    END AS economic_order_quantity,
    -- Optimal number of orders per year
    CASE 
        WHEN ad.total_demand > 0 THEN
            ROUND(ad.total_demand / SQRT(
                2 * ad.total_demand * cp.ordering_cost / 
                (cp.unit_cost * cp.holding_cost_rate)
            ), 1)
        ELSE 0 
    END AS orders_per_year,
    -- Time between orders (days)
    CASE 
        WHEN ad.total_demand > 0 THEN
            ROUND(365 / (ad.total_demand / SQRT(
                2 * ad.total_demand * cp.ordering_cost / 
                (cp.unit_cost * cp.holding_cost_rate)
            )), 0)
        ELSE NULL 
    END AS days_between_orders,
    -- Total annual inventory cost at EOQ
    CASE 
        WHEN ad.total_demand > 0 THEN
            ROUND(SQRT(2 * ad.total_demand * cp.ordering_cost * cp.unit_cost * cp.holding_cost_rate), 2)
        ELSE 0 
    END AS optimal_annual_cost
FROM cost_params cp
LEFT JOIN annual_demand ad ON cp.product_id = ad.product_id
WHERE ad.total_demand > 0
ORDER BY ad.total_demand DESC;


-- ============================================================================
-- 7. INVENTORY CARRYING COST ANALYSIS
-- Detailed breakdown of inventory holding costs
-- ============================================================================

SELECT 
    w.warehouse_code,
    w.warehouse_name,
    pc.category_name,
    COUNT(DISTINCT i.product_id) AS product_count,
    SUM(i.quantity_on_hand) AS total_units,
    SUM(i.quantity_on_hand * p.unit_cost) AS inventory_value,
    -- Cost components (assumed rates)
    ROUND(SUM(i.quantity_on_hand * p.unit_cost) * 0.08, 2) AS capital_cost,        -- 8% opportunity cost
    ROUND(SUM(i.quantity_on_hand * p.unit_cost) * 0.05, 2) AS storage_cost,        -- 5% storage
    ROUND(SUM(i.quantity_on_hand * p.unit_cost) * 0.03, 2) AS insurance_cost,      -- 3% insurance
    ROUND(SUM(i.quantity_on_hand * p.unit_cost) * 0.02, 2) AS obsolescence_risk,   -- 2% obsolescence
    ROUND(SUM(i.quantity_on_hand * p.unit_cost) * 0.02, 2) AS handling_cost,       -- 2% handling
    -- Total carrying cost (20% annually)
    ROUND(SUM(i.quantity_on_hand * p.unit_cost) * 0.20, 2) AS total_carrying_cost,
    -- Monthly carrying cost
    ROUND(SUM(i.quantity_on_hand * p.unit_cost) * 0.20 / 12, 2) AS monthly_carrying_cost
FROM inventory i
JOIN products p ON i.product_id = p.product_id
JOIN warehouses w ON i.warehouse_id = w.warehouse_id
LEFT JOIN product_categories pc ON p.category_id = pc.category_id
WHERE i.quantity_on_hand > 0
GROUP BY w.warehouse_code, w.warehouse_name, pc.category_name
ORDER BY total_carrying_cost DESC;


-- ============================================================================
-- 8. FILL RATE & SERVICE LEVEL METRICS
-- Measures order fulfillment performance
-- ============================================================================

WITH order_fulfillment AS (
    SELECT 
        so.warehouse_id,
        so.so_id,
        soi.product_id,
        soi.quantity_ordered,
        soi.quantity_shipped,
        CASE WHEN soi.quantity_shipped >= soi.quantity_ordered THEN 1 ELSE 0 END AS fully_fulfilled,
        CASE WHEN soi.quantity_shipped > 0 THEN 1 ELSE 0 END AS partially_fulfilled
    FROM sales_orders so
    JOIN sales_order_items soi ON so.so_id = soi.so_id
    WHERE so.status NOT IN ('Cancelled', 'Pending')
        AND so.order_date >= CURRENT_DATE - INTERVAL '3 months'
)
SELECT 
    w.warehouse_code,
    w.warehouse_name,
    COUNT(DISTINCT of.so_id) AS total_orders,
    COUNT(of.product_id) AS total_line_items,
    SUM(of.quantity_ordered) AS total_units_ordered,
    SUM(of.quantity_shipped) AS total_units_shipped,
    -- Unit Fill Rate
    ROUND(SUM(of.quantity_shipped)::DECIMAL / NULLIF(SUM(of.quantity_ordered), 0) * 100, 2) AS unit_fill_rate,
    -- Line Fill Rate (% of lines fully fulfilled)
    ROUND(SUM(of.fully_fulfilled)::DECIMAL / COUNT(*) * 100, 2) AS line_fill_rate,
    -- Order Fill Rate (% of orders with all lines fulfilled)
    ROUND(
        COUNT(DISTINCT CASE WHEN of.fully_fulfilled = 1 THEN of.so_id END)::DECIMAL / 
        NULLIF(COUNT(DISTINCT of.so_id), 0) * 100
    , 2) AS order_fill_rate,
    -- Service Level Grade
    CASE 
        WHEN SUM(of.quantity_shipped)::DECIMAL / NULLIF(SUM(of.quantity_ordered), 0) >= 0.98 THEN 'A'
        WHEN SUM(of.quantity_shipped)::DECIMAL / NULLIF(SUM(of.quantity_ordered), 0) >= 0.95 THEN 'B'
        WHEN SUM(of.quantity_shipped)::DECIMAL / NULLIF(SUM(of.quantity_ordered), 0) >= 0.90 THEN 'C'
        ELSE 'D'
    END AS service_grade
FROM order_fulfillment of
JOIN warehouses w ON of.warehouse_id = w.warehouse_id
GROUP BY w.warehouse_code, w.warehouse_name
ORDER BY unit_fill_rate DESC;


-- ============================================================================
-- 9. LEAD TIME VARIABILITY ANALYSIS
-- Analyzes supplier lead time consistency
-- ============================================================================

WITH lead_times AS (
    SELECT 
        po.supplier_id,
        s.supplier_name,
        poi.product_id,
        p.sku,
        p.product_name,
        po.order_date,
        po.actual_delivery_date,
        (po.actual_delivery_date - po.order_date) AS actual_lead_time_days,
        p.lead_time_days AS expected_lead_time
    FROM purchase_orders po
    JOIN purchase_order_items poi ON po.po_id = poi.po_id
    JOIN products p ON poi.product_id = p.product_id
    JOIN suppliers s ON po.supplier_id = s.supplier_id
    WHERE po.actual_delivery_date IS NOT NULL
        AND po.status = 'Delivered'
        AND po.order_date >= CURRENT_DATE - INTERVAL '12 months'
)
SELECT 
    supplier_id,
    supplier_name,
    product_id,
    sku,
    product_name,
    COUNT(*) AS delivery_count,
    expected_lead_time,
    ROUND(AVG(actual_lead_time_days), 1) AS avg_actual_lead_time,
    MIN(actual_lead_time_days) AS min_lead_time,
    MAX(actual_lead_time_days) AS max_lead_time,
    ROUND(STDDEV(actual_lead_time_days), 2) AS lead_time_stddev,
    -- Coefficient of Variation (CV) - lower is more consistent
    ROUND(STDDEV(actual_lead_time_days) / NULLIF(AVG(actual_lead_time_days), 0) * 100, 2) AS lead_time_cv_pct,
    -- Lead time reliability category
    CASE 
        WHEN STDDEV(actual_lead_time_days) / NULLIF(AVG(actual_lead_time_days), 0) <= 0.10 THEN 'Highly Reliable'
        WHEN STDDEV(actual_lead_time_days) / NULLIF(AVG(actual_lead_time_days), 0) <= 0.25 THEN 'Reliable'
        WHEN STDDEV(actual_lead_time_days) / NULLIF(AVG(actual_lead_time_days), 0) <= 0.40 THEN 'Variable'
        ELSE 'Unreliable'
    END AS reliability_category
FROM lead_times
GROUP BY supplier_id, supplier_name, product_id, sku, product_name, expected_lead_time
HAVING COUNT(*) >= 3
ORDER BY lead_time_cv_pct;


-- ============================================================================
-- 10. DEMAND FORECASTING ACCURACY
-- Measures forecast performance with MAPE and bias metrics
-- ============================================================================

WITH forecast_vs_actual AS (
    SELECT 
        df.product_id,
        df.warehouse_id,
        df.forecast_date,
        df.forecasted_quantity,
        df.model_used,
        COALESCE(SUM(soi.quantity_ordered), 0) AS actual_quantity
    FROM demand_forecasts df
    LEFT JOIN sales_orders so ON df.warehouse_id = so.warehouse_id 
        AND df.forecast_date = so.order_date
        AND so.status NOT IN ('Cancelled')
    LEFT JOIN sales_order_items soi ON so.so_id = soi.so_id 
        AND df.product_id = soi.product_id
    WHERE df.forecast_date BETWEEN CURRENT_DATE - INTERVAL '90 days' AND CURRENT_DATE
        AND df.forecast_period = 'Daily'
    GROUP BY df.product_id, df.warehouse_id, df.forecast_date, df.forecasted_quantity, df.model_used
)
SELECT 
    p.sku,
    p.product_name,
    w.warehouse_code,
    fva.model_used,
    COUNT(*) AS forecast_periods,
    -- Mean Absolute Percentage Error (MAPE)
    ROUND(AVG(
        CASE 
            WHEN fva.actual_quantity > 0 
            THEN ABS(fva.forecasted_quantity - fva.actual_quantity)::DECIMAL / fva.actual_quantity * 100
            ELSE NULL 
        END
    ), 2) AS mape,
    -- Mean Absolute Error (MAE)
    ROUND(AVG(ABS(fva.forecasted_quantity - fva.actual_quantity)), 2) AS mae,
    -- Bias (positive = over-forecasting, negative = under-forecasting)
    ROUND(AVG(fva.forecasted_quantity - fva.actual_quantity), 2) AS forecast_bias,
    -- Forecast accuracy grade
    CASE 
        WHEN AVG(
            CASE 
                WHEN fva.actual_quantity > 0 
                THEN ABS(fva.forecasted_quantity - fva.actual_quantity)::DECIMAL / fva.actual_quantity * 100
                ELSE NULL 
            END
        ) <= 10 THEN 'Excellent'
        WHEN AVG(
            CASE 
                WHEN fva.actual_quantity > 0 
                THEN ABS(fva.forecasted_quantity - fva.actual_quantity)::DECIMAL / fva.actual_quantity * 100
                ELSE NULL 
            END
        ) <= 20 THEN 'Good'
        WHEN AVG(
            CASE 
                WHEN fva.actual_quantity > 0 
                THEN ABS(fva.forecasted_quantity - fva.actual_quantity)::DECIMAL / fva.actual_quantity * 100
                ELSE NULL 
            END
        ) <= 30 THEN 'Fair'
        ELSE 'Poor'
    END AS accuracy_grade
FROM forecast_vs_actual fva
JOIN products p ON fva.product_id = p.product_id
JOIN warehouses w ON fva.warehouse_id = w.warehouse_id
GROUP BY p.sku, p.product_name, w.warehouse_code, fva.model_used
ORDER BY mape;
