"""
Tableau Data Export Module
==========================
Generates optimized data extracts for Tableau dashboards.
Produces denormalized, analytics-ready CSV/Hyper files.

Usage:
    python tableau_export.py --output-dir ./tableau_extracts
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import argparse
import json


class TableauExporter:
    """Generates Tableau-optimized data extracts from supply chain data."""
    
    def __init__(self, data_dir: str = "../data"):
        self.data_dir = Path(data_dir)
        self._load_data()
        
    def _load_data(self):
        """Load all source data files."""
        print("Loading source data...")
        self.products = pd.read_csv(self.data_dir / "products.csv")
        self.categories = pd.read_csv(self.data_dir / "product_categories.csv")
        self.suppliers = pd.read_csv(self.data_dir / "suppliers.csv")
        self.warehouses = pd.read_csv(self.data_dir / "warehouses.csv")
        self.customers = pd.read_csv(self.data_dir / "customers.csv")
        self.inventory = pd.read_csv(self.data_dir / "inventory.csv")
        self.sales_orders = pd.read_csv(self.data_dir / "sales_orders.csv", parse_dates=['order_date'])
        self.sales_items = pd.read_csv(self.data_dir / "sales_order_items.csv")
        self.purchase_orders = pd.read_csv(self.data_dir / "purchase_orders.csv", 
                                           parse_dates=['order_date', 'expected_delivery_date', 'actual_delivery_date'])
        self.po_items = pd.read_csv(self.data_dir / "purchase_order_items.csv")
        self.stockouts = pd.read_csv(self.data_dir / "stockout_events.csv",
                                     parse_dates=['stockout_start_date', 'stockout_end_date'])
        print(f"  Loaded {len(self.products)} products, {len(self.sales_orders)} sales orders")
        
    def _calculate_abc_classification(self) -> pd.DataFrame:
        """Calculate ABC classification for products."""
        # Calculate revenue per product
        sales_with_price = self.sales_items.copy()
        sales_with_price['line_total'] = (
            sales_with_price['quantity_ordered'] * 
            sales_with_price['unit_price'] * 
            (1 - sales_with_price['discount_percent'] / 100)
        )
        
        product_revenue = sales_with_price.groupby('product_id').agg({
            'line_total': 'sum',
            'quantity_ordered': 'sum'
        }).reset_index()
        product_revenue.columns = ['product_id', 'total_revenue', 'total_quantity']
        
        # Sort and calculate cumulative percentage
        product_revenue = product_revenue.sort_values('total_revenue', ascending=False)
        product_revenue['cumulative_revenue'] = product_revenue['total_revenue'].cumsum()
        total = product_revenue['total_revenue'].sum()
        product_revenue['cumulative_pct'] = product_revenue['cumulative_revenue'] / total * 100
        
        # Assign ABC class
        product_revenue['abc_class'] = pd.cut(
            product_revenue['cumulative_pct'],
            bins=[0, 80, 95, 100.01],
            labels=['A', 'B', 'C']
        )
        
        return product_revenue[['product_id', 'total_revenue', 'total_quantity', 'cumulative_pct', 'abc_class']]
    
    def _calculate_supplier_scores(self) -> pd.DataFrame:
        """Calculate supplier reliability scores."""
        # Merge PO with items
        po_analysis = self.purchase_orders.merge(
            self.po_items.groupby('po_id').agg({
                'quantity_ordered': 'sum',
                'quantity_received': 'sum'
            }).reset_index(),
            on='po_id'
        )
        
        # Calculate metrics per supplier
        supplier_metrics = po_analysis.groupby('supplier_id').agg({
            'po_id': 'count',
            'quantity_ordered': 'sum',
            'quantity_received': 'sum'
        }).reset_index()
        supplier_metrics.columns = ['supplier_id', 'total_orders', 'total_ordered', 'total_received']
        
        # On-time delivery
        po_analysis['is_on_time'] = po_analysis['actual_delivery_date'] <= po_analysis['expected_delivery_date']
        on_time = po_analysis.groupby('supplier_id')['is_on_time'].mean().reset_index()
        on_time.columns = ['supplier_id', 'on_time_rate']
        
        # Lead time stats
        po_analysis['lead_time'] = (po_analysis['actual_delivery_date'] - po_analysis['order_date']).dt.days
        lead_time = po_analysis.groupby('supplier_id')['lead_time'].agg(['mean', 'std']).reset_index()
        lead_time.columns = ['supplier_id', 'avg_lead_time', 'lead_time_std']
        lead_time['lead_time_std'] = lead_time['lead_time_std'].fillna(0)
        
        # Merge all metrics
        supplier_scores = supplier_metrics.merge(on_time, on='supplier_id')
        supplier_scores = supplier_scores.merge(lead_time, on='supplier_id')
        
        # Calculate fill rate and composite score
        supplier_scores['fill_rate'] = supplier_scores['total_received'] / supplier_scores['total_ordered']
        supplier_scores['consistency_score'] = 1 - (supplier_scores['lead_time_std'] / supplier_scores['avg_lead_time'].clip(lower=1))
        supplier_scores['consistency_score'] = supplier_scores['consistency_score'].clip(0, 1)
        
        # Composite reliability score (0-100)
        supplier_scores['reliability_score'] = (
            supplier_scores['on_time_rate'] * 40 +
            supplier_scores['fill_rate'] * 40 +
            supplier_scores['consistency_score'] * 20
        )
        
        # Tier assignment
        def assign_tier(row):
            if row['on_time_rate'] >= 0.95 and row['fill_rate'] >= 0.98:
                return 'Platinum'
            elif row['on_time_rate'] >= 0.90 and row['fill_rate'] >= 0.95:
                return 'Gold'
            elif row['on_time_rate'] >= 0.80 and row['fill_rate'] >= 0.90:
                return 'Silver'
            else:
                return 'Bronze'
        
        supplier_scores['tier'] = supplier_scores.apply(assign_tier, axis=1)
        
        return supplier_scores
    
    def export_sales_extract(self, output_dir: Path) -> str:
        """
        Export denormalized sales data for Tableau.
        Grain: One row per sales order line item with all dimensions joined.
        """
        print("Generating Sales Extract...")
        
        # Get ABC classification
        abc = self._calculate_abc_classification()
        
        # Build denormalized table
        sales = self.sales_items.merge(
            self.sales_orders[['so_id', 'customer_id', 'warehouse_id', 'order_date', 'status']],
            on='so_id'
        )
        sales = sales.merge(
            self.products[['product_id', 'sku', 'product_name', 'category_id', 'unit_cost']],
            on='product_id'
        )
        sales = sales.merge(
            self.categories[['category_id', 'category_name']],
            on='category_id'
        )
        sales = sales.merge(
            self.customers[['customer_id', 'customer_name', 'customer_type', 'city', 'state']],
            on='customer_id',
            suffixes=('', '_customer')
        )
        sales = sales.merge(
            self.warehouses[['warehouse_id', 'warehouse_name', 'city', 'state']],
            on='warehouse_id',
            suffixes=('', '_warehouse')
        )
        sales = sales.merge(
            abc[['product_id', 'abc_class', 'cumulative_pct']],
            on='product_id',
            how='left'
        )
        
        # Calculate measures
        sales['gross_amount'] = sales['quantity_ordered'] * sales['unit_price']
        sales['discount_amount'] = sales['gross_amount'] * sales['discount_percent'] / 100
        sales['net_amount'] = sales['gross_amount'] - sales['discount_amount']
        sales['cost_amount'] = sales['quantity_ordered'] * sales['unit_cost']
        sales['profit_amount'] = sales['net_amount'] - sales['cost_amount']
        sales['profit_margin_pct'] = (sales['profit_amount'] / sales['net_amount'] * 100).round(2)
        
        # Add date dimensions
        sales['order_year'] = sales['order_date'].dt.year
        sales['order_month'] = sales['order_date'].dt.month
        sales['order_month_name'] = sales['order_date'].dt.month_name()
        sales['order_quarter'] = sales['order_date'].dt.quarter
        sales['order_week'] = sales['order_date'].dt.isocalendar().week
        sales['order_day_of_week'] = sales['order_date'].dt.day_name()
        sales['is_weekend'] = sales['order_date'].dt.dayofweek >= 5
        
        # Rename for clarity
        sales = sales.rename(columns={
            'city': 'customer_city',
            'state': 'customer_state',
            'city_warehouse': 'warehouse_city',
            'state_warehouse': 'warehouse_state'
        })
        
        # Select final columns
        output_columns = [
            'so_id', 'so_item_id', 'order_date', 'order_year', 'order_month', 
            'order_month_name', 'order_quarter', 'order_week', 'order_day_of_week', 'is_weekend',
            'sku', 'product_name', 'category_name', 'abc_class',
            'customer_name', 'customer_type', 'customer_city', 'customer_state',
            'warehouse_name', 'warehouse_city', 'warehouse_state',
            'quantity_ordered', 'quantity_shipped', 'unit_price', 'unit_cost',
            'discount_percent', 'gross_amount', 'discount_amount', 'net_amount',
            'cost_amount', 'profit_amount', 'profit_margin_pct', 'status'
        ]
        
        sales_export = sales[[c for c in output_columns if c in sales.columns]]
        
        output_path = output_dir / "tableau_sales_extract.csv"
        sales_export.to_csv(output_path, index=False)
        print(f"  Exported {len(sales_export):,} rows to {output_path}")
        
        return str(output_path)
    
    def export_inventory_extract(self, output_dir: Path) -> str:
        """
        Export inventory health data for Tableau.
        Includes current stock levels, turnover metrics, and reorder status.
        """
        print("Generating Inventory Extract...")
        
        # Get ABC classification
        abc = self._calculate_abc_classification()
        
        # Calculate daily demand
        sales_with_date = self.sales_items.merge(
            self.sales_orders[['so_id', 'order_date']],
            on='so_id'
        )
        date_range = (sales_with_date['order_date'].max() - sales_with_date['order_date'].min()).days
        
        daily_demand = sales_with_date.groupby('product_id').agg({
            'quantity_ordered': 'sum'
        }).reset_index()
        daily_demand['avg_daily_demand'] = daily_demand['quantity_ordered'] / max(date_range, 1)
        daily_demand = daily_demand[['product_id', 'avg_daily_demand']]
        
        # Build inventory extract
        inventory = self.inventory.merge(
            self.products[['product_id', 'sku', 'product_name', 'category_id', 'unit_cost', 'unit_price', 'lead_time_days']],
            on='product_id'
        )
        inventory = inventory.merge(
            self.categories[['category_id', 'category_name']],
            on='category_id'
        )
        inventory = inventory.merge(
            self.warehouses[['warehouse_id', 'warehouse_name', 'city', 'state']],
            on='warehouse_id'
        )
        inventory = inventory.merge(
            abc[['product_id', 'abc_class', 'total_revenue']],
            on='product_id',
            how='left'
        )
        inventory = inventory.merge(
            daily_demand,
            on='product_id',
            how='left'
        )
        
        # Calculate metrics
        inventory['quantity_available'] = inventory['quantity_on_hand'] - inventory['quantity_reserved']
        inventory['inventory_value'] = inventory['quantity_on_hand'] * inventory['unit_cost']
        inventory['days_of_supply'] = (
            inventory['quantity_available'] / inventory['avg_daily_demand'].clip(lower=0.01)
        ).round(1)
        
        # Annual carrying cost (20% of inventory value)
        inventory['annual_carrying_cost'] = inventory['inventory_value'] * 0.20
        
        # Status flags
        inventory['is_below_reorder'] = inventory['quantity_available'] < inventory['reorder_point']
        inventory['is_stockout'] = inventory['quantity_available'] <= 0
        inventory['is_overstock'] = inventory['days_of_supply'] > 90
        
        # Status category
        def get_status(row):
            if row['is_stockout']:
                return 'Out of Stock'
            elif row['is_below_reorder']:
                return 'Below Reorder Point'
            elif row['is_overstock']:
                return 'Overstock'
            else:
                return 'Healthy'
        
        inventory['inventory_status'] = inventory.apply(get_status, axis=1)
        
        # Turnover classification
        def get_movement_class(dos):
            if dos < 30:
                return 'Fast Moving'
            elif dos < 90:
                return 'Normal'
            elif dos < 180:
                return 'Slow Moving'
            else:
                return 'Dead Stock'
        
        inventory['movement_class'] = inventory['days_of_supply'].apply(get_movement_class)
        
        output_columns = [
            'warehouse_name', 'city', 'state',
            'sku', 'product_name', 'category_name', 'abc_class',
            'quantity_on_hand', 'quantity_reserved', 'quantity_available',
            'reorder_point', 'reorder_quantity',
            'unit_cost', 'inventory_value', 'annual_carrying_cost',
            'avg_daily_demand', 'days_of_supply',
            'inventory_status', 'movement_class',
            'is_below_reorder', 'is_stockout', 'is_overstock',
            'total_revenue'
        ]
        
        inventory_export = inventory[[c for c in output_columns if c in inventory.columns]]
        
        output_path = output_dir / "tableau_inventory_extract.csv"
        inventory_export.to_csv(output_path, index=False)
        print(f"  Exported {len(inventory_export):,} rows to {output_path}")
        
        return str(output_path)
    
    def export_supplier_extract(self, output_dir: Path) -> str:
        """
        Export supplier performance data for Tableau.
        Includes reliability scores, lead times, and tier classifications.
        """
        print("Generating Supplier Extract...")
        
        # Get supplier scores
        scores = self._calculate_supplier_scores()
        
        # Merge with supplier details
        supplier_export = scores.merge(
            self.suppliers[['supplier_id', 'supplier_name', 'city', 'state', 'country', 
                           'payment_terms_days', 'quality_rating', 'is_preferred']],
            on='supplier_id'
        )
        
        # Calculate spend
        po_spend = self.purchase_orders.merge(
            self.po_items[['po_id', 'quantity_ordered', 'unit_cost']],
            on='po_id'
        )
        po_spend['line_total'] = po_spend['quantity_ordered'] * po_spend['unit_cost']
        supplier_spend = po_spend.groupby('supplier_id')['line_total'].sum().reset_index()
        supplier_spend.columns = ['supplier_id', 'total_spend']
        
        supplier_export = supplier_export.merge(supplier_spend, on='supplier_id', how='left')
        
        # Format percentages
        supplier_export['on_time_rate_pct'] = (supplier_export['on_time_rate'] * 100).round(1)
        supplier_export['fill_rate_pct'] = (supplier_export['fill_rate'] * 100).round(1)
        supplier_export['reliability_score'] = supplier_export['reliability_score'].round(1)
        supplier_export['avg_lead_time'] = supplier_export['avg_lead_time'].round(1)
        
        # Performance rating
        def get_performance_rating(row):
            if row['reliability_score'] >= 90:
                return 'Excellent'
            elif row['reliability_score'] >= 75:
                return 'Good'
            elif row['reliability_score'] >= 60:
                return 'Fair'
            else:
                return 'Needs Improvement'
        
        supplier_export['performance_rating'] = supplier_export.apply(get_performance_rating, axis=1)
        
        output_columns = [
            'supplier_id', 'supplier_name', 'city', 'state', 'country',
            'payment_terms_days', 'quality_rating', 'is_preferred',
            'total_orders', 'total_spend',
            'on_time_rate_pct', 'fill_rate_pct', 'reliability_score',
            'avg_lead_time', 'lead_time_std',
            'tier', 'performance_rating'
        ]
        
        supplier_export = supplier_export[[c for c in output_columns if c in supplier_export.columns]]
        
        output_path = output_dir / "tableau_supplier_extract.csv"
        supplier_export.to_csv(output_path, index=False)
        print(f"  Exported {len(supplier_export):,} rows to {output_path}")
        
        return str(output_path)
    
    def export_time_series_extract(self, output_dir: Path) -> str:
        """
        Export time series data for trend analysis in Tableau.
        Grain: Daily aggregates by warehouse and product category.
        """
        print("Generating Time Series Extract...")
        
        # Aggregate sales by date, warehouse, category
        sales = self.sales_items.merge(
            self.sales_orders[['so_id', 'warehouse_id', 'order_date']],
            on='so_id'
        )
        sales = sales.merge(
            self.products[['product_id', 'category_id', 'unit_cost']],
            on='product_id'
        )
        sales = sales.merge(
            self.categories[['category_id', 'category_name']],
            on='category_id'
        )
        sales = sales.merge(
            self.warehouses[['warehouse_id', 'warehouse_name']],
            on='warehouse_id'
        )
        
        # Calculate line totals
        sales['revenue'] = sales['quantity_ordered'] * sales['unit_price'] * (1 - sales['discount_percent'] / 100)
        sales['cost'] = sales['quantity_ordered'] * sales['unit_cost']
        sales['profit'] = sales['revenue'] - sales['cost']
        
        # Daily aggregation
        daily = sales.groupby(['order_date', 'warehouse_name', 'category_name']).agg({
            'so_id': 'nunique',
            'quantity_ordered': 'sum',
            'revenue': 'sum',
            'cost': 'sum',
            'profit': 'sum'
        }).reset_index()
        daily.columns = ['date', 'warehouse', 'category', 'order_count', 'units_sold', 'revenue', 'cost', 'profit']
        
        # Add date dimensions
        daily['year'] = daily['date'].dt.year
        daily['month'] = daily['date'].dt.month
        daily['month_name'] = daily['date'].dt.month_name()
        daily['quarter'] = daily['date'].dt.quarter
        daily['week'] = daily['date'].dt.isocalendar().week
        daily['day_of_week'] = daily['date'].dt.day_name()
        daily['is_weekend'] = daily['date'].dt.dayofweek >= 5
        
        # Calculate moving averages (will be done in Tableau, but pre-calculate 7-day)
        daily = daily.sort_values(['warehouse', 'category', 'date'])
        daily['revenue_7d_ma'] = daily.groupby(['warehouse', 'category'])['revenue'].transform(
            lambda x: x.rolling(7, min_periods=1).mean()
        ).round(2)
        
        output_path = output_dir / "tableau_timeseries_extract.csv"
        daily.to_csv(output_path, index=False)
        print(f"  Exported {len(daily):,} rows to {output_path}")
        
        return str(output_path)
    
    def export_all(self, output_dir: str = "./tableau_extracts") -> dict:
        """Export all Tableau extracts."""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        print(f"\n{'='*60}")
        print("TABLEAU DATA EXPORT")
        print(f"{'='*60}")
        print(f"Output directory: {output_path.absolute()}\n")
        
        exports = {
            'sales': self.export_sales_extract(output_path),
            'inventory': self.export_inventory_extract(output_path),
            'supplier': self.export_supplier_extract(output_path),
            'timeseries': self.export_time_series_extract(output_path)
        }
        
        # Generate metadata file
        metadata = {
            'export_timestamp': datetime.now().isoformat(),
            'source_data_dir': str(self.data_dir.absolute()),
            'exports': exports,
            'tableau_usage': {
                'sales_extract': 'Connect as data source for sales dashboards. Grain: line item level.',
                'inventory_extract': 'Connect for inventory health dashboards. Grain: product-warehouse level.',
                'supplier_extract': 'Connect for supplier scorecard. Grain: supplier level.',
                'timeseries_extract': 'Connect for trend analysis. Grain: daily by warehouse/category.'
            },
            'recommended_relationships': [
                'Link sales and inventory on [sku]',
                'Link supplier to inventory via product-supplier mapping',
                'Use timeseries for trend lines and forecasting'
            ]
        }
        
        metadata_path = output_path / "export_metadata.json"
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        print(f"\n{'='*60}")
        print("EXPORT COMPLETE")
        print(f"{'='*60}")
        print(f"Files exported to: {output_path.absolute()}")
        print(f"Metadata saved to: {metadata_path}")
        print("\nNext steps:")
        print("1. Open Tableau Desktop")
        print("2. Connect to CSV files in the export directory")
        print("3. Create relationships between extracts using common keys")
        print("4. Build dashboards using pre-calculated metrics")
        
        return exports


def main():
    parser = argparse.ArgumentParser(description='Export data for Tableau dashboards')
    parser.add_argument('--data-dir', default='../data', help='Source data directory')
    parser.add_argument('--output-dir', default='./tableau_extracts', help='Output directory for extracts')
    args = parser.parse_args()
    
    exporter = TableauExporter(data_dir=args.data_dir)
    exporter.export_all(output_dir=args.output_dir)


if __name__ == "__main__":
    main()
