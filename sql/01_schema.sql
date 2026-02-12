-- Supply Chain Optimization & Inventory Analytics Database Schema
-- Author: Krishna Somisetty
-- Description: Comprehensive schema for supply chain management and inventory optimization

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- DIMENSION TABLES
-- ============================================================================

-- Warehouses/Distribution Centers
CREATE TABLE warehouses (
    warehouse_id SERIAL PRIMARY KEY,
    warehouse_code VARCHAR(10) UNIQUE NOT NULL,
    warehouse_name VARCHAR(100) NOT NULL,
    address VARCHAR(255),
    city VARCHAR(100) NOT NULL,
    state VARCHAR(50) NOT NULL,
    zip_code VARCHAR(20),
    country VARCHAR(50) DEFAULT 'USA',
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    capacity_units INTEGER NOT NULL,
    operating_cost_per_unit DECIMAL(10, 2),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Product Categories
CREATE TABLE product_categories (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL,
    parent_category_id INTEGER REFERENCES product_categories(category_id),
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Products
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    sku VARCHAR(50) UNIQUE NOT NULL,
    product_name VARCHAR(200) NOT NULL,
    category_id INTEGER REFERENCES product_categories(category_id),
    unit_cost DECIMAL(12, 2) NOT NULL,
    unit_price DECIMAL(12, 2) NOT NULL,
    weight_kg DECIMAL(8, 3),
    volume_cubic_m DECIMAL(8, 4),
    lead_time_days INTEGER DEFAULT 7,
    safety_stock_days INTEGER DEFAULT 3,
    min_order_quantity INTEGER DEFAULT 1,
    is_perishable BOOLEAN DEFAULT FALSE,
    shelf_life_days INTEGER,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Suppliers
CREATE TABLE suppliers (
    supplier_id SERIAL PRIMARY KEY,
    supplier_code VARCHAR(20) UNIQUE NOT NULL,
    supplier_name VARCHAR(200) NOT NULL,
    contact_name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20),
    address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(50),
    payment_terms_days INTEGER DEFAULT 30,
    is_preferred BOOLEAN DEFAULT FALSE,
    quality_rating DECIMAL(3, 2) CHECK (quality_rating BETWEEN 0 AND 5),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Product-Supplier Mapping (which suppliers can provide which products)
CREATE TABLE product_suppliers (
    product_supplier_id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES products(product_id),
    supplier_id INTEGER REFERENCES suppliers(supplier_id),
    supplier_sku VARCHAR(50),
    unit_cost DECIMAL(12, 2) NOT NULL,
    min_order_quantity INTEGER DEFAULT 1,
    lead_time_days INTEGER NOT NULL,
    is_primary_supplier BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(product_id, supplier_id)
);

-- Customers
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    customer_code VARCHAR(20) UNIQUE NOT NULL,
    customer_name VARCHAR(200) NOT NULL,
    customer_type VARCHAR(50) CHECK (customer_type IN ('Retail', 'Wholesale', 'Distributor', 'Direct')),
    email VARCHAR(100),
    phone VARCHAR(20),
    address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    country VARCHAR(50) DEFAULT 'USA',
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    credit_limit DECIMAL(12, 2),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- TRANSACTION TABLES
-- ============================================================================

-- Purchase Orders (from suppliers)
CREATE TABLE purchase_orders (
    po_id SERIAL PRIMARY KEY,
    po_number VARCHAR(30) UNIQUE NOT NULL,
    supplier_id INTEGER REFERENCES suppliers(supplier_id),
    warehouse_id INTEGER REFERENCES warehouses(warehouse_id),
    order_date DATE NOT NULL,
    expected_delivery_date DATE NOT NULL,
    actual_delivery_date DATE,
    status VARCHAR(30) CHECK (status IN ('Draft', 'Submitted', 'Confirmed', 'Shipped', 'Delivered', 'Cancelled')),
    total_amount DECIMAL(14, 2),
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Purchase Order Line Items
CREATE TABLE purchase_order_items (
    po_item_id SERIAL PRIMARY KEY,
    po_id INTEGER REFERENCES purchase_orders(po_id),
    product_id INTEGER REFERENCES products(product_id),
    quantity_ordered INTEGER NOT NULL,
    quantity_received INTEGER DEFAULT 0,
    unit_cost DECIMAL(12, 2) NOT NULL,
    line_total DECIMAL(14, 2) GENERATED ALWAYS AS (quantity_ordered * unit_cost) STORED,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Sales Orders (to customers)
CREATE TABLE sales_orders (
    so_id SERIAL PRIMARY KEY,
    so_number VARCHAR(30) UNIQUE NOT NULL,
    customer_id INTEGER REFERENCES customers(customer_id),
    warehouse_id INTEGER REFERENCES warehouses(warehouse_id),
    order_date DATE NOT NULL,
    requested_delivery_date DATE,
    actual_ship_date DATE,
    status VARCHAR(30) CHECK (status IN ('Pending', 'Confirmed', 'Picking', 'Shipped', 'Delivered', 'Cancelled')),
    total_amount DECIMAL(14, 2),
    shipping_cost DECIMAL(10, 2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Sales Order Line Items
CREATE TABLE sales_order_items (
    so_item_id SERIAL PRIMARY KEY,
    so_id INTEGER REFERENCES sales_orders(so_id),
    product_id INTEGER REFERENCES products(product_id),
    quantity_ordered INTEGER NOT NULL,
    quantity_shipped INTEGER DEFAULT 0,
    unit_price DECIMAL(12, 2) NOT NULL,
    discount_percent DECIMAL(5, 2) DEFAULT 0,
    line_total DECIMAL(14, 2) GENERATED ALWAYS AS (quantity_ordered * unit_price * (1 - discount_percent/100)) STORED,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Inventory Levels (current stock at each warehouse)
CREATE TABLE inventory (
    inventory_id SERIAL PRIMARY KEY,
    warehouse_id INTEGER REFERENCES warehouses(warehouse_id),
    product_id INTEGER REFERENCES products(product_id),
    quantity_on_hand INTEGER NOT NULL DEFAULT 0,
    quantity_reserved INTEGER NOT NULL DEFAULT 0,
    quantity_available INTEGER GENERATED ALWAYS AS (quantity_on_hand - quantity_reserved) STORED,
    reorder_point INTEGER,
    reorder_quantity INTEGER,
    last_counted_date DATE,
    last_received_date DATE,
    last_sold_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(warehouse_id, product_id)
);

-- Inventory Transactions (movement history)
CREATE TABLE inventory_transactions (
    transaction_id SERIAL PRIMARY KEY,
    warehouse_id INTEGER REFERENCES warehouses(warehouse_id),
    product_id INTEGER REFERENCES products(product_id),
    transaction_type VARCHAR(30) CHECK (transaction_type IN ('Receipt', 'Sale', 'Transfer_In', 'Transfer_Out', 'Adjustment', 'Return', 'Damaged')),
    quantity INTEGER NOT NULL,
    reference_type VARCHAR(30),
    reference_id INTEGER,
    unit_cost DECIMAL(12, 2),
    transaction_date TIMESTAMP NOT NULL,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Demand Forecast
CREATE TABLE demand_forecasts (
    forecast_id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES products(product_id),
    warehouse_id INTEGER REFERENCES warehouses(warehouse_id),
    forecast_date DATE NOT NULL,
    forecast_period VARCHAR(20) CHECK (forecast_period IN ('Daily', 'Weekly', 'Monthly')),
    forecasted_quantity INTEGER NOT NULL,
    confidence_lower INTEGER,
    confidence_upper INTEGER,
    model_used VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(product_id, warehouse_id, forecast_date, forecast_period)
);

-- Stockout Events
CREATE TABLE stockout_events (
    stockout_id SERIAL PRIMARY KEY,
    warehouse_id INTEGER REFERENCES warehouses(warehouse_id),
    product_id INTEGER REFERENCES products(product_id),
    stockout_start_date TIMESTAMP NOT NULL,
    stockout_end_date TIMESTAMP,
    demand_during_stockout INTEGER DEFAULT 0,
    lost_sales_amount DECIMAL(14, 2),
    root_cause VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================================

CREATE INDEX idx_inventory_warehouse ON inventory(warehouse_id);
CREATE INDEX idx_inventory_product ON inventory(product_id);
CREATE INDEX idx_inv_trans_date ON inventory_transactions(transaction_date);
CREATE INDEX idx_inv_trans_product ON inventory_transactions(product_id);
CREATE INDEX idx_po_supplier ON purchase_orders(supplier_id);
CREATE INDEX idx_po_date ON purchase_orders(order_date);
CREATE INDEX idx_so_customer ON sales_orders(customer_id);
CREATE INDEX idx_so_date ON sales_orders(order_date);
CREATE INDEX idx_products_category ON products(category_id);
CREATE INDEX idx_forecast_product_date ON demand_forecasts(product_id, forecast_date);

-- ============================================================================
-- VIEWS FOR ANALYTICS
-- ============================================================================

-- Current Inventory Status with ABC Classification
CREATE OR REPLACE VIEW v_inventory_status AS
SELECT 
    i.inventory_id,
    w.warehouse_code,
    w.warehouse_name,
    p.sku,
    p.product_name,
    pc.category_name,
    i.quantity_on_hand,
    i.quantity_reserved,
    i.quantity_available,
    i.reorder_point,
    p.unit_cost,
    (i.quantity_on_hand * p.unit_cost) AS inventory_value,
    CASE 
        WHEN i.quantity_available <= 0 THEN 'Out of Stock'
        WHEN i.quantity_available <= i.reorder_point THEN 'Reorder'
        WHEN i.quantity_available <= i.reorder_point * 1.5 THEN 'Low Stock'
        ELSE 'In Stock'
    END AS stock_status,
    i.last_received_date,
    i.last_sold_date
FROM inventory i
JOIN warehouses w ON i.warehouse_id = w.warehouse_id
JOIN products p ON i.product_id = p.product_id
LEFT JOIN product_categories pc ON p.category_id = pc.category_id;

-- Supplier Performance Summary
CREATE OR REPLACE VIEW v_supplier_performance AS
SELECT 
    s.supplier_id,
    s.supplier_code,
    s.supplier_name,
    COUNT(po.po_id) AS total_orders,
    COUNT(CASE WHEN po.status = 'Delivered' THEN 1 END) AS delivered_orders,
    AVG(CASE WHEN po.actual_delivery_date IS NOT NULL 
        THEN po.actual_delivery_date - po.expected_delivery_date END) AS avg_delivery_variance_days,
    COUNT(CASE WHEN po.actual_delivery_date <= po.expected_delivery_date THEN 1 END)::DECIMAL / 
        NULLIF(COUNT(CASE WHEN po.actual_delivery_date IS NOT NULL THEN 1 END), 0) * 100 AS on_time_delivery_rate,
    SUM(po.total_amount) AS total_purchase_value,
    s.quality_rating
FROM suppliers s
LEFT JOIN purchase_orders po ON s.supplier_id = po.supplier_id
GROUP BY s.supplier_id, s.supplier_code, s.supplier_name, s.quality_rating;

-- Daily Sales Summary
CREATE OR REPLACE VIEW v_daily_sales AS
SELECT 
    so.order_date,
    w.warehouse_code,
    p.sku,
    p.product_name,
    SUM(soi.quantity_ordered) AS units_sold,
    SUM(soi.line_total) AS revenue,
    SUM(soi.quantity_ordered * p.unit_cost) AS cost_of_goods_sold,
    SUM(soi.line_total) - SUM(soi.quantity_ordered * p.unit_cost) AS gross_profit
FROM sales_orders so
JOIN sales_order_items soi ON so.so_id = soi.so_id
JOIN products p ON soi.product_id = p.product_id
JOIN warehouses w ON so.warehouse_id = w.warehouse_id
WHERE so.status NOT IN ('Cancelled')
GROUP BY so.order_date, w.warehouse_code, p.sku, p.product_name;
