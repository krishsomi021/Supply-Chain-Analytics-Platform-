"""
Supply Chain Synthetic Data Generator
Author: Krishna Somisetty
Description: Generates realistic synthetic data for supply chain analytics project
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

# Set random seed for reproducibility
np.random.seed(42)
random.seed(42)

class SupplyChainDataGenerator:
    """Generates synthetic supply chain data with realistic patterns"""
    
    def __init__(self, output_dir: str = "data"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # Configuration
        self.n_warehouses = 5
        self.n_products = 200
        self.n_suppliers = 25
        self.n_customers = 150
        self.date_start = datetime(2024, 1, 1)
        self.date_end = datetime(2025, 12, 31)
        
        # Generated data storage
        self.warehouses = None
        self.categories = None
        self.products = None
        self.suppliers = None
        self.product_suppliers = None
        self.customers = None
        self.purchase_orders = None
        self.purchase_order_items = None
        self.sales_orders = None
        self.sales_order_items = None
        self.inventory = None
        self.inventory_transactions = None
        self.demand_forecasts = None
        self.stockout_events = None
    
    def generate_all(self):
        """Generate all synthetic data"""
        print("Generating supply chain data...")
        
        self._generate_warehouses()
        self._generate_categories()
        self._generate_products()
        self._generate_suppliers()
        self._generate_product_suppliers()
        self._generate_customers()
        self._generate_purchase_orders()
        self._generate_sales_orders()
        self._generate_inventory()
        self._generate_inventory_transactions()
        self._generate_demand_forecasts()
        self._generate_stockout_events()
        
        self._save_all()
        print(f"Data generation complete! Files saved to {self.output_dir}/")
        
        return self
    
    def _generate_warehouses(self):
        """Generate warehouse/distribution center data"""
        warehouse_data = [
            ("WH001", "East Coast DC", "Newark", "NJ", 40.7357, -74.1724),
            ("WH002", "Midwest Hub", "Chicago", "IL", 41.8781, -87.6298),
            ("WH003", "West Coast DC", "Los Angeles", "CA", 34.0522, -118.2437),
            ("WH004", "Southeast DC", "Atlanta", "GA", 33.7490, -84.3880),
            ("WH005", "Texas Regional", "Dallas", "TX", 32.7767, -96.7970),
        ]
        
        self.warehouses = pd.DataFrame(warehouse_data, columns=[
            "warehouse_code", "warehouse_name", "city", "state", "latitude", "longitude"
        ])
        self.warehouses["warehouse_id"] = range(1, len(self.warehouses) + 1)
        self.warehouses["capacity_units"] = np.random.randint(50000, 150000, len(self.warehouses))
        self.warehouses["operating_cost_per_unit"] = np.round(np.random.uniform(2.5, 5.0, len(self.warehouses)), 2)
        self.warehouses["is_active"] = True
        
        print(f"  Generated {len(self.warehouses)} warehouses")
    
    def _generate_categories(self):
        """Generate product category hierarchy"""
        category_data = [
            (1, "Electronics", None),
            (2, "Home & Garden", None),
            (3, "Automotive", None),
            (4, "Office Supplies", None),
            (5, "Health & Beauty", None),
            (6, "Computers", 1),
            (7, "Mobile Devices", 1),
            (8, "Audio Equipment", 1),
            (9, "Furniture", 2),
            (10, "Kitchen", 2),
            (11, "Tools", 2),
            (12, "Parts", 3),
            (13, "Accessories", 3),
            (14, "Paper Products", 4),
            (15, "Writing Supplies", 4),
        ]
        
        self.categories = pd.DataFrame(category_data, columns=[
            "category_id", "category_name", "parent_category_id"
        ])
        
        print(f"  Generated {len(self.categories)} categories")
    
    def _generate_products(self):
        """Generate product catalog with ABC distribution"""
        products_list = []
        
        # Product name components
        prefixes = ["Premium", "Standard", "Economy", "Professional", "Ultra", "Basic", "Advanced"]
        items = [
            "Laptop", "Monitor", "Keyboard", "Mouse", "Headphones", "Speaker", "Tablet", "Phone Case",
            "Chair", "Desk", "Lamp", "Shelf", "Cabinet", "Drawer", "Pan", "Pot", "Mixer", "Blender",
            "Drill", "Hammer", "Screwdriver Set", "Wrench", "Level", "Tape Measure",
            "Brake Pad", "Oil Filter", "Air Filter", "Battery", "Wiper Blade",
            "Paper Ream", "Notebook", "Pen Set", "Stapler", "Folder", "Binder",
            "Shampoo", "Lotion", "Vitamins", "First Aid Kit", "Thermometer"
        ]
        
        # Leaf categories for assignment
        leaf_categories = [6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
        
        for i in range(1, self.n_products + 1):
            prefix = random.choice(prefixes)
            item = random.choice(items)
            variant = random.choice(["A", "B", "C", "Pro", "Plus", "Lite", "Max", ""])
            
            # ABC distribution: 20% high-value, 30% medium, 50% low
            abc_rand = random.random()
            if abc_rand < 0.20:  # A-class items (high value, high demand)
                unit_cost = round(random.uniform(100, 500), 2)
                base_demand = random.randint(50, 200)
            elif abc_rand < 0.50:  # B-class items
                unit_cost = round(random.uniform(30, 100), 2)
                base_demand = random.randint(20, 80)
            else:  # C-class items
                unit_cost = round(random.uniform(5, 30), 2)
                base_demand = random.randint(5, 30)
            
            products_list.append({
                "product_id": i,
                "sku": f"SKU-{i:05d}",
                "product_name": f"{prefix} {item} {variant}".strip(),
                "category_id": random.choice(leaf_categories),
                "unit_cost": unit_cost,
                "unit_price": round(unit_cost * random.uniform(1.3, 2.0), 2),
                "weight_kg": round(random.uniform(0.1, 25), 3),
                "volume_cubic_m": round(random.uniform(0.001, 0.5), 4),
                "lead_time_days": random.randint(3, 21),
                "safety_stock_days": random.randint(2, 7),
                "min_order_quantity": random.choice([1, 5, 10, 25, 50]),
                "is_perishable": random.random() < 0.1,
                "is_active": True,
                "base_daily_demand": base_demand  # For simulation
            })
        
        self.products = pd.DataFrame(products_list)
        print(f"  Generated {len(self.products)} products")
    
    def _generate_suppliers(self):
        """Generate supplier master data"""
        company_names = [
            "Global Parts Inc", "Prime Distributors", "Quality Goods Co", "FastShip Supplies",
            "Reliable Wholesale", "DirectSource Ltd", "ValueChain Partners", "Industrial Depot",
            "MegaSupply Corp", "TradeMaster Inc", "Summit Distributors", "Pacific Trading",
            "Atlantic Supply Chain", "Central Wholesale", "Premier Components", "Alliance Goods",
            "NextGen Suppliers", "ProSource Direct", "United Distributors", "EcoSupply Co",
            "TechParts Unlimited", "National Wholesale", "Express Components", "BulkBuy Inc",
            "StarLogistics"
        ]
        
        suppliers_list = []
        for i, name in enumerate(company_names[:self.n_suppliers], 1):
            # Simulate supplier reliability - some are very reliable, some less so
            reliability = random.random()
            
            suppliers_list.append({
                "supplier_id": i,
                "supplier_code": f"SUP-{i:03d}",
                "supplier_name": name,
                "contact_name": f"Contact {i}",
                "email": f"contact@{name.lower().replace(' ', '')}.com",
                "phone": f"555-{random.randint(100,999)}-{random.randint(1000,9999)}",
                "city": random.choice(["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]),
                "state": random.choice(["NY", "CA", "IL", "TX", "AZ"]),
                "country": "USA",
                "payment_terms_days": random.choice([15, 30, 45, 60]),
                "is_preferred": reliability > 0.7,
                "quality_rating": round(3.0 + reliability * 2, 2),  # 3.0 - 5.0 range
                "is_active": True,
                "reliability_factor": reliability  # Internal use for simulation
            })
        
        self.suppliers = pd.DataFrame(suppliers_list)
        print(f"  Generated {len(self.suppliers)} suppliers")
    
    def _generate_product_suppliers(self):
        """Generate product-supplier mappings"""
        ps_list = []
        ps_id = 1
        
        for _, product in self.products.iterrows():
            # Each product has 1-3 suppliers
            n_suppliers = random.randint(1, 3)
            supplier_ids = random.sample(range(1, self.n_suppliers + 1), n_suppliers)
            
            for idx, sup_id in enumerate(supplier_ids):
                supplier = self.suppliers[self.suppliers.supplier_id == sup_id].iloc[0]
                
                # Primary supplier usually has better terms
                is_primary = idx == 0
                lead_time_variation = -2 if is_primary else random.randint(0, 5)
                cost_variation = 1.0 if is_primary else random.uniform(1.0, 1.15)
                
                ps_list.append({
                    "product_supplier_id": ps_id,
                    "product_id": product.product_id,
                    "supplier_id": sup_id,
                    "supplier_sku": f"{supplier.supplier_code}-{product.sku}",
                    "unit_cost": round(product.unit_cost * cost_variation, 2),
                    "min_order_quantity": product.min_order_quantity,
                    "lead_time_days": max(1, product.lead_time_days + lead_time_variation),
                    "is_primary_supplier": is_primary
                })
                ps_id += 1
        
        self.product_suppliers = pd.DataFrame(ps_list)
        print(f"  Generated {len(self.product_suppliers)} product-supplier mappings")
    
    def _generate_customers(self):
        """Generate customer master data"""
        customer_types = ["Retail", "Wholesale", "Distributor", "Direct"]
        cities = [
            ("New York", "NY", 40.7128, -74.0060),
            ("Los Angeles", "CA", 34.0522, -118.2437),
            ("Chicago", "IL", 41.8781, -87.6298),
            ("Houston", "TX", 29.7604, -95.3698),
            ("Phoenix", "AZ", 33.4484, -112.0740),
            ("Philadelphia", "PA", 39.9526, -75.1652),
            ("San Antonio", "TX", 29.4241, -98.4936),
            ("San Diego", "CA", 32.7157, -117.1611),
            ("Dallas", "TX", 32.7767, -96.7970),
            ("San Jose", "CA", 37.3382, -121.8863),
            ("Austin", "TX", 30.2672, -97.7431),
            ("Jacksonville", "FL", 30.3322, -81.6557),
            ("Fort Worth", "TX", 32.7555, -97.3308),
            ("Columbus", "OH", 39.9612, -82.9988),
            ("Charlotte", "NC", 35.2271, -80.8431),
        ]
        
        customers_list = []
        for i in range(1, self.n_customers + 1):
            city_info = random.choice(cities)
            cust_type = random.choice(customer_types)
            
            # Larger customers (wholesale/distributor) have higher credit limits
            if cust_type in ["Wholesale", "Distributor"]:
                credit_limit = round(random.uniform(100000, 500000), 2)
            else:
                credit_limit = round(random.uniform(10000, 100000), 2)
            
            customers_list.append({
                "customer_id": i,
                "customer_code": f"CUST-{i:04d}",
                "customer_name": f"Customer {i} {cust_type}",
                "customer_type": cust_type,
                "email": f"orders@customer{i}.com",
                "phone": f"555-{random.randint(100,999)}-{random.randint(1000,9999)}",
                "city": city_info[0],
                "state": city_info[1],
                "zip_code": f"{random.randint(10000, 99999)}",
                "country": "USA",
                "latitude": city_info[2] + random.uniform(-0.5, 0.5),
                "longitude": city_info[3] + random.uniform(-0.5, 0.5),
                "credit_limit": credit_limit,
                "is_active": True
            })
        
        self.customers = pd.DataFrame(customers_list)
        print(f"  Generated {len(self.customers)} customers")
    
    def _generate_purchase_orders(self):
        """Generate purchase order history"""
        po_list = []
        poi_list = []
        po_id = 1
        poi_id = 1
        
        current_date = self.date_start
        
        while current_date <= self.date_end:
            # Generate 5-15 POs per day
            n_pos = random.randint(5, 15)
            
            for _ in range(n_pos):
                supplier = self.suppliers.sample(1).iloc[0]
                warehouse = self.warehouses.sample(1).iloc[0]
                
                # Get products this supplier can provide
                supplier_products = self.product_suppliers[
                    self.product_suppliers.supplier_id == supplier.supplier_id
                ]
                
                if len(supplier_products) == 0:
                    continue
                
                # Order 1-5 different products
                n_items = min(random.randint(1, 5), len(supplier_products))
                items = supplier_products.sample(n_items)
                
                # Calculate delivery dates based on supplier reliability
                base_lead_time = items.lead_time_days.mean()
                expected_delivery = current_date + timedelta(days=int(base_lead_time))
                
                # Actual delivery varies by supplier reliability
                reliability = supplier.reliability_factor
                delivery_variance = int((1 - reliability) * 7)  # Less reliable = more variance
                actual_variance = random.randint(-2, delivery_variance)
                actual_delivery = expected_delivery + timedelta(days=actual_variance)
                
                # Determine status based on dates
                if actual_delivery > self.date_end:
                    status = random.choice(["Submitted", "Confirmed", "Shipped"])
                    actual_delivery = None
                else:
                    status = "Delivered"
                
                # Create PO
                total_amount = 0
                po_items = []
                
                for _, item in items.iterrows():
                    product = self.products[self.products.product_id == item.product_id].iloc[0]
                    qty = random.randint(50, 500) * product.min_order_quantity
                    qty = max(qty, item.min_order_quantity)
                    
                    line_total = qty * item.unit_cost
                    total_amount += line_total
                    
                    # Quantity received may be less due to quality issues
                    if status == "Delivered":
                        qty_received = int(qty * random.uniform(0.95, 1.0))
                    else:
                        qty_received = 0
                    
                    po_items.append({
                        "po_item_id": poi_id,
                        "po_id": po_id,
                        "product_id": item.product_id,
                        "quantity_ordered": qty,
                        "quantity_received": qty_received,
                        "unit_cost": item.unit_cost
                    })
                    poi_id += 1
                
                po_list.append({
                    "po_id": po_id,
                    "po_number": f"PO-{current_date.year}-{po_id:06d}",
                    "supplier_id": supplier.supplier_id,
                    "warehouse_id": warehouse.warehouse_id,
                    "order_date": current_date,
                    "expected_delivery_date": expected_delivery,
                    "actual_delivery_date": actual_delivery,
                    "status": status,
                    "total_amount": round(total_amount, 2)
                })
                
                poi_list.extend(po_items)
                po_id += 1
            
            current_date += timedelta(days=1)
        
        self.purchase_orders = pd.DataFrame(po_list)
        self.purchase_order_items = pd.DataFrame(poi_list)
        print(f"  Generated {len(self.purchase_orders)} purchase orders with {len(self.purchase_order_items)} line items")
    
    def _generate_sales_orders(self):
        """Generate sales order history with seasonal patterns"""
        so_list = []
        soi_list = []
        so_id = 1
        soi_id = 1
        
        current_date = self.date_start
        
        while current_date <= self.date_end:
            # Seasonal demand multiplier
            month = current_date.month
            if month in [11, 12]:  # Holiday season
                seasonal_mult = 1.8
            elif month in [1, 2]:  # Post-holiday slump
                seasonal_mult = 0.7
            elif month in [6, 7, 8]:  # Summer
                seasonal_mult = 1.2
            else:
                seasonal_mult = 1.0
            
            # Day of week effect (lower on weekends)
            dow = current_date.weekday()
            if dow >= 5:  # Weekend
                dow_mult = 0.4
            else:
                dow_mult = 1.0
            
            # Base orders per day
            base_orders = 50
            n_orders = int(base_orders * seasonal_mult * dow_mult * random.uniform(0.8, 1.2))
            
            for _ in range(n_orders):
                customer = self.customers.sample(1).iloc[0]
                warehouse = self.warehouses.sample(1).iloc[0]
                
                # Customer type affects order size
                if customer.customer_type in ["Wholesale", "Distributor"]:
                    n_items = random.randint(5, 15)
                    qty_mult = random.randint(5, 20)
                else:
                    n_items = random.randint(1, 5)
                    qty_mult = random.randint(1, 3)
                
                # Select products (biased toward A-class items)
                products_sample = self.products.sample(n_items, weights='base_daily_demand')
                
                total_amount = 0
                so_items = []
                
                for _, product in products_sample.iterrows():
                    qty = random.randint(1, 10) * qty_mult
                    discount = random.choice([0, 0, 0, 5, 10, 15]) if customer.customer_type != "Direct" else 0
                    line_total = qty * product.unit_price * (1 - discount/100)
                    total_amount += line_total
                    
                    # Determine shipped quantity
                    order_age = (self.date_end - current_date).days
                    if order_age > 7:
                        status = "Delivered"
                        qty_shipped = qty
                    elif order_age > 3:
                        status = "Shipped"
                        qty_shipped = qty
                    else:
                        status = random.choice(["Pending", "Confirmed", "Picking"])
                        qty_shipped = 0
                    
                    so_items.append({
                        "so_item_id": soi_id,
                        "so_id": so_id,
                        "product_id": product.product_id,
                        "quantity_ordered": qty,
                        "quantity_shipped": qty_shipped,
                        "unit_price": product.unit_price,
                        "discount_percent": discount
                    })
                    soi_id += 1
                
                requested_delivery = current_date + timedelta(days=random.randint(3, 14))
                actual_ship = current_date + timedelta(days=random.randint(1, 5)) if status in ["Shipped", "Delivered"] else None
                
                so_list.append({
                    "so_id": so_id,
                    "so_number": f"SO-{current_date.year}-{so_id:06d}",
                    "customer_id": customer.customer_id,
                    "warehouse_id": warehouse.warehouse_id,
                    "order_date": current_date,
                    "requested_delivery_date": requested_delivery,
                    "actual_ship_date": actual_ship,
                    "status": status,
                    "total_amount": round(total_amount, 2),
                    "shipping_cost": round(random.uniform(10, 100), 2)
                })
                
                soi_list.extend(so_items)
                so_id += 1
            
            current_date += timedelta(days=1)
        
        self.sales_orders = pd.DataFrame(so_list)
        self.sales_order_items = pd.DataFrame(soi_list)
        print(f"  Generated {len(self.sales_orders)} sales orders with {len(self.sales_order_items)} line items")
    
    def _generate_inventory(self):
        """Generate current inventory levels"""
        inv_list = []
        inv_id = 1
        
        for _, warehouse in self.warehouses.iterrows():
            for _, product in self.products.iterrows():
                # Calculate typical demand
                avg_daily = product.base_daily_demand / self.n_warehouses
                
                # Reorder point based on lead time and safety stock
                reorder_point = int(avg_daily * (product.lead_time_days + product.safety_stock_days))
                reorder_qty = int(avg_daily * 30)  # ~1 month supply
                
                # Current quantity - some randomization
                qty_on_hand = int(reorder_point * random.uniform(0.5, 3.0))
                qty_reserved = int(qty_on_hand * random.uniform(0, 0.3))
                
                inv_list.append({
                    "inventory_id": inv_id,
                    "warehouse_id": warehouse.warehouse_id,
                    "product_id": product.product_id,
                    "quantity_on_hand": qty_on_hand,
                    "quantity_reserved": qty_reserved,
                    "reorder_point": reorder_point,
                    "reorder_quantity": reorder_qty,
                    "last_received_date": self.date_end - timedelta(days=random.randint(1, 30)),
                    "last_sold_date": self.date_end - timedelta(days=random.randint(0, 14))
                })
                inv_id += 1
        
        self.inventory = pd.DataFrame(inv_list)
        print(f"  Generated {len(self.inventory)} inventory records")
    
    def _generate_inventory_transactions(self):
        """Generate inventory movement history"""
        trans_list = []
        trans_id = 1
        
        # Generate from delivered POs
        for _, po in self.purchase_orders[self.purchase_orders.status == "Delivered"].iterrows():
            po_items = self.purchase_order_items[self.purchase_order_items.po_id == po.po_id]
            
            for _, item in po_items.iterrows():
                if item.quantity_received > 0:
                    product = self.products[self.products.product_id == item.product_id].iloc[0]
                    trans_list.append({
                        "transaction_id": trans_id,
                        "warehouse_id": po.warehouse_id,
                        "product_id": item.product_id,
                        "transaction_type": "Receipt",
                        "quantity": item.quantity_received,
                        "reference_type": "PO",
                        "reference_id": po.po_id,
                        "unit_cost": item.unit_cost,
                        "transaction_date": po.actual_delivery_date
                    })
                    trans_id += 1
        
        # Generate from shipped SOs
        for _, so in self.sales_orders[self.sales_orders.status.isin(["Shipped", "Delivered"])].iterrows():
            so_items = self.sales_order_items[self.sales_order_items.so_id == so.so_id]
            
            for _, item in so_items.iterrows():
                if item.quantity_shipped > 0:
                    product = self.products[self.products.product_id == item.product_id].iloc[0]
                    trans_list.append({
                        "transaction_id": trans_id,
                        "warehouse_id": so.warehouse_id,
                        "product_id": item.product_id,
                        "transaction_type": "Sale",
                        "quantity": -item.quantity_shipped,
                        "reference_type": "SO",
                        "reference_id": so.so_id,
                        "unit_cost": product.unit_cost,
                        "transaction_date": so.actual_ship_date
                    })
                    trans_id += 1
        
        self.inventory_transactions = pd.DataFrame(trans_list)
        print(f"  Generated {len(self.inventory_transactions)} inventory transactions")
    
    def _generate_demand_forecasts(self):
        """Generate demand forecast data"""
        forecast_list = []
        forecast_id = 1
        
        # Generate daily forecasts for next 30 days
        forecast_start = self.date_end - timedelta(days=90)
        forecast_end = self.date_end + timedelta(days=30)
        
        for _, warehouse in self.warehouses.iterrows():
            for _, product in self.products.iterrows():
                avg_demand = product.base_daily_demand / self.n_warehouses
                current_date = forecast_start
                
                while current_date <= forecast_end:
                    # Add some seasonality and trend
                    month_factor = 1.0 + 0.3 * np.sin(2 * np.pi * current_date.month / 12)
                    trend = 1.0 + (current_date - forecast_start).days * 0.001
                    
                    base_forecast = avg_demand * month_factor * trend
                    forecast_qty = max(0, int(base_forecast + np.random.normal(0, base_forecast * 0.2)))
                    
                    # Confidence interval
                    conf_width = int(forecast_qty * 0.3)
                    
                    forecast_list.append({
                        "forecast_id": forecast_id,
                        "product_id": product.product_id,
                        "warehouse_id": warehouse.warehouse_id,
                        "forecast_date": current_date,
                        "forecast_period": "Daily",
                        "forecasted_quantity": forecast_qty,
                        "confidence_lower": max(0, forecast_qty - conf_width),
                        "confidence_upper": forecast_qty + conf_width,
                        "model_used": random.choice(["ARIMA", "Prophet", "LightGBM", "Exponential_Smoothing"])
                    })
                    forecast_id += 1
                    current_date += timedelta(days=1)
        
        self.demand_forecasts = pd.DataFrame(forecast_list)
        print(f"  Generated {len(self.demand_forecasts)} demand forecasts")
    
    def _generate_stockout_events(self):
        """Generate stockout event history"""
        stockout_list = []
        stockout_id = 1
        
        # Identify low-inventory items and create stockout events
        for _, inv in self.inventory.iterrows():
            product = self.products[self.products.product_id == inv.product_id].iloc[0]
            
            # Products with low stock relative to demand may have had stockouts
            if inv.quantity_on_hand < inv.reorder_point * 0.5:
                # Generate 1-3 stockout events for this product
                n_events = random.randint(1, 3)
                
                for _ in range(n_events):
                    start_date = self.date_start + timedelta(days=random.randint(30, 300))
                    duration = random.randint(1, 7)
                    end_date = start_date + timedelta(days=duration)
                    
                    daily_demand = product.base_daily_demand / self.n_warehouses
                    lost_demand = int(daily_demand * duration * random.uniform(0.5, 1.5))
                    lost_sales = round(lost_demand * product.unit_price, 2)
                    
                    root_causes = [
                        "Supplier delay", "Demand spike", "Forecast error", 
                        "Quality issue", "Transportation delay", "System error"
                    ]
                    
                    stockout_list.append({
                        "stockout_id": stockout_id,
                        "warehouse_id": inv.warehouse_id,
                        "product_id": inv.product_id,
                        "stockout_start_date": start_date,
                        "stockout_end_date": end_date,
                        "demand_during_stockout": lost_demand,
                        "lost_sales_amount": lost_sales,
                        "root_cause": random.choice(root_causes)
                    })
                    stockout_id += 1
        
        self.stockout_events = pd.DataFrame(stockout_list)
        print(f"  Generated {len(self.stockout_events)} stockout events")
    
    def _save_all(self):
        """Save all generated data to CSV files"""
        datasets = {
            "warehouses": self.warehouses,
            "product_categories": self.categories,
            "products": self.products.drop(columns=["base_daily_demand"]),
            "suppliers": self.suppliers.drop(columns=["reliability_factor"]),
            "product_suppliers": self.product_suppliers,
            "customers": self.customers,
            "purchase_orders": self.purchase_orders,
            "purchase_order_items": self.purchase_order_items,
            "sales_orders": self.sales_orders,
            "sales_order_items": self.sales_order_items,
            "inventory": self.inventory,
            "inventory_transactions": self.inventory_transactions,
            "demand_forecasts": self.demand_forecasts,
            "stockout_events": self.stockout_events
        }
        
        for name, df in datasets.items():
            filepath = os.path.join(self.output_dir, f"{name}.csv")
            df.to_csv(filepath, index=False)
            print(f"  Saved {filepath}")
    
    def get_summary(self) -> dict:
        """Return summary statistics of generated data"""
        return {
            "warehouses": len(self.warehouses),
            "products": len(self.products),
            "suppliers": len(self.suppliers),
            "customers": len(self.customers),
            "purchase_orders": len(self.purchase_orders),
            "purchase_order_items": len(self.purchase_order_items),
            "sales_orders": len(self.sales_orders),
            "sales_order_items": len(self.sales_order_items),
            "inventory_records": len(self.inventory),
            "inventory_transactions": len(self.inventory_transactions),
            "demand_forecasts": len(self.demand_forecasts),
            "stockout_events": len(self.stockout_events),
            "date_range": f"{self.date_start.date()} to {self.date_end.date()}"
        }


if __name__ == "__main__":
    generator = SupplyChainDataGenerator(output_dir="data")
    generator.generate_all()
    
    print("\n" + "="*50)
    print("DATA GENERATION SUMMARY")
    print("="*50)
    for key, value in generator.get_summary().items():
        print(f"  {key}: {value}")
