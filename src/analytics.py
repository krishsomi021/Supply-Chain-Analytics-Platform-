"""
Supply Chain Optimization & Analytics Engine
Author: Krishna Somisetty
Description: Core analytics and optimization algorithms for supply chain management
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')


@dataclass
class InventoryMetrics:
    """Container for inventory performance metrics"""
    product_id: int
    sku: str
    warehouse_id: int
    current_stock: int
    reorder_point: int
    safety_stock: int
    economic_order_qty: int
    days_of_supply: float
    turnover_ratio: float
    carrying_cost: float
    abc_class: str
    stockout_risk: str


@dataclass
class SupplierScore:
    """Container for supplier performance scores"""
    supplier_id: int
    supplier_name: str
    on_time_delivery_rate: float
    fill_rate: float
    lead_time_variance: float
    quality_score: float
    reliability_score: float
    tier: str


class InventoryAnalytics:
    """Comprehensive inventory analytics and optimization"""
    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self._load_data()
    
    def _load_data(self):
        """Load all required datasets"""
        self.products = pd.read_csv(f"{self.data_dir}/products.csv")
        self.inventory = pd.read_csv(f"{self.data_dir}/inventory.csv")
        self.sales_orders = pd.read_csv(f"{self.data_dir}/sales_orders.csv", parse_dates=['order_date'])
        self.sales_order_items = pd.read_csv(f"{self.data_dir}/sales_order_items.csv")
        self.purchase_orders = pd.read_csv(f"{self.data_dir}/purchase_orders.csv", 
                                           parse_dates=['order_date', 'expected_delivery_date', 'actual_delivery_date'])
        self.purchase_order_items = pd.read_csv(f"{self.data_dir}/purchase_order_items.csv")
        self.suppliers = pd.read_csv(f"{self.data_dir}/suppliers.csv")
        self.warehouses = pd.read_csv(f"{self.data_dir}/warehouses.csv")
        self.stockout_events = pd.read_csv(f"{self.data_dir}/stockout_events.csv", 
                                           parse_dates=['stockout_start_date', 'stockout_end_date'])
    
    def calculate_abc_classification(self, 
                                     lookback_days: int = 365,
                                     a_threshold: float = 80,
                                     b_threshold: float = 95) -> pd.DataFrame:
        """
        Perform ABC classification based on revenue contribution.
        
        Args:
            lookback_days: Number of days to analyze
            a_threshold: Cumulative revenue % for A-class
            b_threshold: Cumulative revenue % for B-class
            
        Returns:
            DataFrame with ABC classification for each product
        """
        cutoff_date = self.sales_orders['order_date'].max() - timedelta(days=lookback_days)
        
        # Join sales data
        recent_sales = self.sales_orders[
            (self.sales_orders['order_date'] >= cutoff_date) & 
            (self.sales_orders['status'] != 'Cancelled')
        ]
        
        sales_with_items = recent_sales.merge(self.sales_order_items, on='so_id')
        
        # Calculate revenue per product
        product_revenue = sales_with_items.groupby('product_id').agg({
            'quantity_ordered': 'sum',
            'unit_price': 'mean'
        }).reset_index()
        
        product_revenue['total_revenue'] = product_revenue['quantity_ordered'] * product_revenue['unit_price']
        product_revenue = product_revenue.sort_values('total_revenue', ascending=False)
        
        # Calculate cumulative percentage
        total_revenue = product_revenue['total_revenue'].sum()
        product_revenue['cumulative_revenue'] = product_revenue['total_revenue'].cumsum()
        product_revenue['cumulative_pct'] = product_revenue['cumulative_revenue'] / total_revenue * 100
        
        # Assign ABC class
        def assign_class(pct):
            if pct <= a_threshold:
                return 'A'
            elif pct <= b_threshold:
                return 'B'
            return 'C'
        
        product_revenue['abc_class'] = product_revenue['cumulative_pct'].apply(assign_class)
        
        # Add product details
        result = product_revenue.merge(
            self.products[['product_id', 'sku', 'product_name', 'unit_cost']],
            on='product_id'
        )
        
        return result[['product_id', 'sku', 'product_name', 'quantity_ordered', 
                       'total_revenue', 'cumulative_pct', 'abc_class']]
    
    def calculate_inventory_turnover(self, 
                                     warehouse_id: Optional[int] = None) -> pd.DataFrame:
        """
        Calculate inventory turnover ratio and days of supply.
        
        Args:
            warehouse_id: Optional filter for specific warehouse
            
        Returns:
            DataFrame with turnover metrics per product/warehouse
        """
        # Filter inventory if warehouse specified
        inv = self.inventory.copy()
        if warehouse_id:
            inv = inv[inv['warehouse_id'] == warehouse_id]
        
        # Calculate COGS from sales
        sales_with_items = self.sales_orders.merge(self.sales_order_items, on='so_id')
        sales_with_items = sales_with_items[sales_with_items['status'] != 'Cancelled']
        
        cogs = sales_with_items.groupby(['warehouse_id', 'product_id']).agg({
            'quantity_ordered': 'sum'
        }).reset_index()
        
        # Merge with products for unit cost
        cogs = cogs.merge(self.products[['product_id', 'unit_cost']], on='product_id')
        cogs['cogs_annual'] = cogs['quantity_ordered'] * cogs['unit_cost']
        
        # Merge with inventory
        turnover = inv.merge(cogs, on=['warehouse_id', 'product_id'], how='left')
        turnover = turnover.merge(self.products[['product_id', 'sku', 'product_name']], on='product_id')
        turnover = turnover.merge(self.warehouses[['warehouse_id', 'warehouse_code']], on='warehouse_id')
        
        # Calculate metrics
        turnover['inventory_value'] = turnover['quantity_on_hand'] * turnover['unit_cost']
        turnover['cogs_annual'] = turnover['cogs_annual'].fillna(0)
        
        # Turnover ratio = COGS / Average Inventory
        turnover['turnover_ratio'] = np.where(
            turnover['inventory_value'] > 0,
            turnover['cogs_annual'] / turnover['inventory_value'],
            0
        )
        
        # Days of supply = 365 / turnover ratio
        turnover['days_of_supply'] = np.where(
            turnover['turnover_ratio'] > 0,
            365 / turnover['turnover_ratio'],
            np.inf
        )
        
        # Classify turnover
        def classify_turnover(ratio):
            if ratio >= 12:
                return 'Fast Moving'
            elif ratio >= 4:
                return 'Normal'
            elif ratio >= 1:
                return 'Slow Moving'
            return 'Dead Stock'
        
        turnover['turnover_category'] = turnover['turnover_ratio'].apply(classify_turnover)
        
        return turnover[['warehouse_code', 'sku', 'product_name', 'quantity_on_hand',
                         'inventory_value', 'cogs_annual', 'turnover_ratio', 
                         'days_of_supply', 'turnover_category']]
    
    def calculate_reorder_points(self, 
                                 service_level: float = 0.95,
                                 lookback_days: int = 90) -> pd.DataFrame:
        """
        Calculate dynamic reorder points based on demand variability and lead time.
        
        Uses the formula: ROP = (Avg Daily Demand × Lead Time) + Safety Stock
        Safety Stock = Z × √(LT × σd² + d² × σLT²)
        
        Args:
            service_level: Target service level (0.95 = 95%)
            lookback_days: Days of history to analyze
            
        Returns:
            DataFrame with calculated reorder points
        """
        # Z-score for service level
        from scipy import stats
        z_score = stats.norm.ppf(service_level)
        
        cutoff_date = self.sales_orders['order_date'].max() - timedelta(days=lookback_days)
        
        # Calculate daily demand statistics
        recent_sales = self.sales_orders[
            (self.sales_orders['order_date'] >= cutoff_date) & 
            (self.sales_orders['status'] != 'Cancelled')
        ].merge(self.sales_order_items, on='so_id')
        
        daily_demand = recent_sales.groupby(
            ['warehouse_id', 'product_id', 'order_date']
        )['quantity_ordered'].sum().reset_index()
        
        demand_stats = daily_demand.groupby(['warehouse_id', 'product_id']).agg({
            'quantity_ordered': ['mean', 'std', 'count']
        }).reset_index()
        demand_stats.columns = ['warehouse_id', 'product_id', 'avg_daily_demand', 
                                'std_daily_demand', 'days_with_demand']
        
        # Fill missing std with 30% of mean (conservative estimate)
        demand_stats['std_daily_demand'] = demand_stats['std_daily_demand'].fillna(
            demand_stats['avg_daily_demand'] * 0.3
        )
        
        # Calculate lead time statistics from POs
        delivered_pos = self.purchase_orders[
            self.purchase_orders['status'] == 'Delivered'
        ].copy()
        delivered_pos['actual_lead_time'] = (
            delivered_pos['actual_delivery_date'] - delivered_pos['order_date']
        ).dt.days
        
        lead_time_stats = delivered_pos.groupby('supplier_id').agg({
            'actual_lead_time': ['mean', 'std']
        }).reset_index()
        lead_time_stats.columns = ['supplier_id', 'avg_lead_time', 'std_lead_time']
        lead_time_stats['std_lead_time'] = lead_time_stats['std_lead_time'].fillna(
            lead_time_stats['avg_lead_time'] * 0.2
        )
        
        # Merge with products (use product's default lead time if no supplier data)
        result = demand_stats.merge(self.inventory, on=['warehouse_id', 'product_id'])
        result = result.merge(self.products[['product_id', 'sku', 'product_name', 
                                             'lead_time_days', 'unit_cost']], on='product_id')
        result = result.merge(self.warehouses[['warehouse_id', 'warehouse_code']], on='warehouse_id')
        
        # Use default lead time if no historical data
        result['avg_lead_time'] = result['lead_time_days']
        result['std_lead_time'] = result['lead_time_days'] * 0.2
        
        # Calculate safety stock
        # SS = z × √(LT × σd² + d² × σLT²)
        result['safety_stock'] = z_score * np.sqrt(
            result['avg_lead_time'] * result['std_daily_demand']**2 +
            result['avg_daily_demand']**2 * result['std_lead_time']**2
        )
        
        # Calculate reorder point
        result['calculated_rop'] = (
            result['avg_daily_demand'] * result['avg_lead_time'] + 
            result['safety_stock']
        )
        
        # Round to integers
        result['safety_stock'] = result['safety_stock'].round(0).astype(int)
        result['calculated_rop'] = result['calculated_rop'].round(0).astype(int)
        
        # Compare with current reorder point
        result['current_rop'] = result['reorder_point']
        result['rop_adjustment'] = result['calculated_rop'] - result['current_rop']
        
        def recommend(row):
            if row['rop_adjustment'] > row['current_rop'] * 0.2:
                return 'Increase ROP'
            elif row['rop_adjustment'] < -row['current_rop'] * 0.2:
                return 'Decrease ROP'
            return 'Optimal'
        
        result['recommendation'] = result.apply(recommend, axis=1)
        
        return result[['warehouse_code', 'sku', 'product_name', 'quantity_on_hand',
                       'avg_daily_demand', 'avg_lead_time', 'safety_stock',
                       'current_rop', 'calculated_rop', 'recommendation']]
    
    def calculate_eoq(self, 
                      ordering_cost: float = 50.0,
                      holding_cost_rate: float = 0.25) -> pd.DataFrame:
        """
        Calculate Economic Order Quantity (EOQ) using Wilson's formula.
        
        EOQ = √(2DS/H)
        D = Annual demand
        S = Ordering cost per order
        H = Holding cost per unit per year
        
        Args:
            ordering_cost: Fixed cost per order placement
            holding_cost_rate: Annual holding cost as % of unit cost
            
        Returns:
            DataFrame with EOQ calculations
        """
        # Calculate annual demand per product
        sales_with_items = self.sales_orders.merge(self.sales_order_items, on='so_id')
        sales_with_items = sales_with_items[sales_with_items['status'] != 'Cancelled']
        
        annual_demand = sales_with_items.groupby('product_id').agg({
            'quantity_ordered': 'sum'
        }).reset_index()
        annual_demand.columns = ['product_id', 'annual_demand']
        
        # Merge with products
        result = annual_demand.merge(
            self.products[['product_id', 'sku', 'product_name', 'unit_cost']],
            on='product_id'
        )
        
        # Calculate holding cost per unit
        result['holding_cost_per_unit'] = result['unit_cost'] * holding_cost_rate
        
        # Calculate EOQ
        result['eoq'] = np.sqrt(
            2 * result['annual_demand'] * ordering_cost / 
            result['holding_cost_per_unit']
        ).round(0).astype(int)
        
        # Orders per year
        result['orders_per_year'] = (
            result['annual_demand'] / result['eoq']
        ).round(1)
        
        # Days between orders
        result['days_between_orders'] = (
            365 / result['orders_per_year']
        ).round(0)
        
        # Total annual inventory cost at EOQ
        result['annual_inventory_cost'] = np.sqrt(
            2 * result['annual_demand'] * ordering_cost * 
            result['holding_cost_per_unit']
        ).round(2)
        
        return result[['sku', 'product_name', 'unit_cost', 'annual_demand',
                       'eoq', 'orders_per_year', 'days_between_orders',
                       'annual_inventory_cost']]
    
    def calculate_carrying_costs(self) -> pd.DataFrame:
        """
        Calculate detailed inventory carrying costs by warehouse and category.
        
        Cost components:
        - Capital cost (opportunity cost): 8%
        - Storage cost: 5%
        - Insurance: 3%
        - Obsolescence risk: 2%
        - Handling: 2%
        Total: 20% of inventory value annually
        
        Returns:
            DataFrame with carrying cost breakdown
        """
        # Merge inventory with products and warehouses
        inv = self.inventory.merge(
            self.products[['product_id', 'sku', 'unit_cost', 'category_id']],
            on='product_id'
        )
        inv = inv.merge(
            self.warehouses[['warehouse_id', 'warehouse_code', 'warehouse_name']],
            on='warehouse_id'
        )
        
        # Load categories
        categories = pd.read_csv(f"{self.data_dir}/product_categories.csv")
        inv = inv.merge(categories[['category_id', 'category_name']], on='category_id', how='left')
        
        # Calculate inventory value
        inv['inventory_value'] = inv['quantity_on_hand'] * inv['unit_cost']
        
        # Aggregate by warehouse and category
        summary = inv.groupby(['warehouse_code', 'warehouse_name', 'category_name']).agg({
            'product_id': 'count',
            'quantity_on_hand': 'sum',
            'inventory_value': 'sum'
        }).reset_index()
        summary.columns = ['warehouse_code', 'warehouse_name', 'category_name',
                          'product_count', 'total_units', 'inventory_value']
        
        # Cost components
        summary['capital_cost'] = summary['inventory_value'] * 0.08
        summary['storage_cost'] = summary['inventory_value'] * 0.05
        summary['insurance_cost'] = summary['inventory_value'] * 0.03
        summary['obsolescence_cost'] = summary['inventory_value'] * 0.02
        summary['handling_cost'] = summary['inventory_value'] * 0.02
        summary['total_carrying_cost'] = summary['inventory_value'] * 0.20
        summary['monthly_carrying_cost'] = summary['total_carrying_cost'] / 12
        
        # Round all money columns
        money_cols = ['inventory_value', 'capital_cost', 'storage_cost', 'insurance_cost',
                     'obsolescence_cost', 'handling_cost', 'total_carrying_cost', 'monthly_carrying_cost']
        summary[money_cols] = summary[money_cols].round(2)
        
        return summary


class SupplierAnalytics:
    """Supplier performance analysis and scoring"""
    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self._load_data()
    
    def _load_data(self):
        """Load required datasets"""
        self.suppliers = pd.read_csv(f"{self.data_dir}/suppliers.csv")
        self.purchase_orders = pd.read_csv(f"{self.data_dir}/purchase_orders.csv",
                                           parse_dates=['order_date', 'expected_delivery_date', 'actual_delivery_date'])
        self.purchase_order_items = pd.read_csv(f"{self.data_dir}/purchase_order_items.csv")
    
    def calculate_supplier_scores(self, 
                                  lookback_days: int = 365) -> pd.DataFrame:
        """
        Calculate comprehensive supplier performance scores.
        
        Metrics:
        - On-time delivery rate (40% weight)
        - Fill rate (40% weight)
        - Lead time consistency (20% weight)
        
        Returns:
            DataFrame with supplier scores and tiers
        """
        cutoff_date = self.purchase_orders['order_date'].max() - timedelta(days=lookback_days)
        
        # Filter delivered orders
        recent_pos = self.purchase_orders[
            (self.purchase_orders['order_date'] >= cutoff_date) &
            (self.purchase_orders['status'] == 'Delivered')
        ].copy()
        
        # Calculate delivery metrics
        recent_pos['is_on_time'] = (
            recent_pos['actual_delivery_date'] <= recent_pos['expected_delivery_date']
        ).astype(int)
        
        recent_pos['delivery_variance'] = (
            recent_pos['actual_delivery_date'] - recent_pos['expected_delivery_date']
        ).dt.days
        
        delivery_metrics = recent_pos.groupby('supplier_id').agg({
            'po_id': 'count',
            'is_on_time': 'sum',
            'delivery_variance': ['mean', 'std']
        }).reset_index()
        delivery_metrics.columns = ['supplier_id', 'total_orders', 'on_time_orders',
                                    'avg_variance', 'std_variance']
        
        delivery_metrics['on_time_rate'] = (
            delivery_metrics['on_time_orders'] / delivery_metrics['total_orders'] * 100
        )
        
        # Calculate fill rate
        po_items = self.purchase_order_items.merge(
            recent_pos[['po_id', 'supplier_id']],
            on='po_id'
        )
        
        fill_metrics = po_items.groupby('supplier_id').agg({
            'quantity_ordered': 'sum',
            'quantity_received': 'sum'
        }).reset_index()
        
        fill_metrics['fill_rate'] = (
            fill_metrics['quantity_received'] / fill_metrics['quantity_ordered'] * 100
        )
        
        # Merge metrics
        scores = delivery_metrics.merge(fill_metrics, on='supplier_id')
        scores = scores.merge(self.suppliers[['supplier_id', 'supplier_code', 
                                              'supplier_name', 'quality_rating']], 
                              on='supplier_id')
        
        # Calculate composite score
        # On-time: 40%, Fill rate: 40%, Consistency: 20%
        def consistency_score(variance_std):
            if pd.isna(variance_std) or variance_std <= 1:
                return 20
            elif variance_std <= 3:
                return 15
            elif variance_std <= 7:
                return 10
            return 5
        
        scores['consistency_score'] = scores['std_variance'].apply(consistency_score)
        
        scores['reliability_score'] = (
            scores['on_time_rate'] * 0.4 +
            scores['fill_rate'] * 0.4 +
            scores['consistency_score']
        )
        
        # Assign tier
        def assign_tier(row):
            if row['on_time_rate'] >= 95 and row['fill_rate'] >= 98:
                return 'Platinum'
            elif row['on_time_rate'] >= 90 and row['fill_rate'] >= 95:
                return 'Gold'
            elif row['on_time_rate'] >= 80 and row['fill_rate'] >= 90:
                return 'Silver'
            return 'Bronze'
        
        scores['tier'] = scores.apply(assign_tier, axis=1)
        
        # Round values
        scores['on_time_rate'] = scores['on_time_rate'].round(2)
        scores['fill_rate'] = scores['fill_rate'].round(2)
        scores['avg_variance'] = scores['avg_variance'].round(1)
        scores['reliability_score'] = scores['reliability_score'].round(1)
        
        return scores[['supplier_code', 'supplier_name', 'total_orders',
                       'on_time_rate', 'fill_rate', 'avg_variance',
                       'quality_rating', 'reliability_score', 'tier']]
    
    def analyze_lead_time_variability(self) -> pd.DataFrame:
        """
        Analyze lead time consistency by supplier.
        
        Returns:
            DataFrame with lead time statistics and reliability classification
        """
        delivered = self.purchase_orders[
            self.purchase_orders['status'] == 'Delivered'
        ].copy()
        
        delivered['actual_lead_time'] = (
            delivered['actual_delivery_date'] - delivered['order_date']
        ).dt.days
        
        lead_time_stats = delivered.groupby('supplier_id').agg({
            'po_id': 'count',
            'actual_lead_time': ['mean', 'std', 'min', 'max']
        }).reset_index()
        lead_time_stats.columns = ['supplier_id', 'delivery_count', 
                                   'avg_lead_time', 'std_lead_time',
                                   'min_lead_time', 'max_lead_time']
        
        # Coefficient of variation
        lead_time_stats['cv_pct'] = (
            lead_time_stats['std_lead_time'] / lead_time_stats['avg_lead_time'] * 100
        )
        
        # Reliability category
        def classify_reliability(cv):
            if pd.isna(cv) or cv <= 10:
                return 'Highly Reliable'
            elif cv <= 25:
                return 'Reliable'
            elif cv <= 40:
                return 'Variable'
            return 'Unreliable'
        
        lead_time_stats['reliability_category'] = lead_time_stats['cv_pct'].apply(classify_reliability)
        
        # Merge supplier info
        lead_time_stats = lead_time_stats.merge(
            self.suppliers[['supplier_id', 'supplier_code', 'supplier_name']],
            on='supplier_id'
        )
        
        # Round values
        for col in ['avg_lead_time', 'std_lead_time', 'cv_pct']:
            lead_time_stats[col] = lead_time_stats[col].round(2)
        
        return lead_time_stats[['supplier_code', 'supplier_name', 'delivery_count',
                                'avg_lead_time', 'std_lead_time', 'min_lead_time',
                                'max_lead_time', 'cv_pct', 'reliability_category']]


class DemandForecaster:
    """Demand forecasting using multiple methods"""
    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self._load_data()
    
    def _load_data(self):
        """Load required datasets"""
        self.sales_orders = pd.read_csv(f"{self.data_dir}/sales_orders.csv", 
                                        parse_dates=['order_date'])
        self.sales_order_items = pd.read_csv(f"{self.data_dir}/sales_order_items.csv")
        self.products = pd.read_csv(f"{self.data_dir}/products.csv")
    
    def prepare_demand_data(self, 
                            product_id: Optional[int] = None,
                            warehouse_id: Optional[int] = None) -> pd.DataFrame:
        """
        Prepare daily demand time series data.
        
        Args:
            product_id: Filter for specific product
            warehouse_id: Filter for specific warehouse
            
        Returns:
            DataFrame with daily demand
        """
        # Join orders with items
        demand = self.sales_orders.merge(self.sales_order_items, on='so_id')
        demand = demand[demand['status'] != 'Cancelled']
        
        # Apply filters
        if product_id:
            demand = demand[demand['product_id'] == product_id]
        if warehouse_id:
            demand = demand[demand['warehouse_id'] == warehouse_id]
        
        # Aggregate by date and product
        daily_demand = demand.groupby(
            ['order_date', 'product_id']
        )['quantity_ordered'].sum().reset_index()
        
        return daily_demand
    
    def moving_average_forecast(self, 
                                product_id: int,
                                window: int = 7,
                                forecast_days: int = 30) -> pd.DataFrame:
        """
        Simple moving average forecast.
        
        Args:
            product_id: Product to forecast
            window: Moving average window
            forecast_days: Days to forecast
            
        Returns:
            DataFrame with forecasts
        """
        demand = self.prepare_demand_data(product_id=product_id)
        
        if len(demand) == 0:
            return pd.DataFrame()
        
        # Create complete date range
        date_range = pd.date_range(
            demand['order_date'].min(),
            demand['order_date'].max()
        )
        
        daily = demand.set_index('order_date').reindex(date_range, fill_value=0)
        daily['quantity_ordered'] = daily['quantity_ordered'].fillna(0)
        
        # Calculate moving average
        daily['ma'] = daily['quantity_ordered'].rolling(window=window, min_periods=1).mean()
        
        # Forecast
        last_ma = daily['ma'].iloc[-1]
        forecast_dates = pd.date_range(
            daily.index.max() + timedelta(days=1),
            periods=forecast_days
        )
        
        forecast = pd.DataFrame({
            'date': forecast_dates,
            'forecasted_quantity': [round(last_ma)] * forecast_days,
            'method': 'Moving Average'
        })
        
        return forecast
    
    def exponential_smoothing_forecast(self,
                                       product_id: int,
                                       alpha: float = 0.3,
                                       forecast_days: int = 30) -> pd.DataFrame:
        """
        Simple exponential smoothing forecast.
        
        Args:
            product_id: Product to forecast
            alpha: Smoothing parameter (0-1)
            forecast_days: Days to forecast
            
        Returns:
            DataFrame with forecasts
        """
        demand = self.prepare_demand_data(product_id=product_id)
        
        if len(demand) == 0:
            return pd.DataFrame()
        
        # Create complete date range
        date_range = pd.date_range(
            demand['order_date'].min(),
            demand['order_date'].max()
        )
        
        daily = demand.set_index('order_date').reindex(date_range, fill_value=0)
        daily['quantity_ordered'] = daily['quantity_ordered'].fillna(0)
        
        # Calculate exponential smoothing
        values = daily['quantity_ordered'].values
        smoothed = [values[0]]
        
        for i in range(1, len(values)):
            smoothed.append(alpha * values[i] + (1 - alpha) * smoothed[-1])
        
        daily['smoothed'] = smoothed
        
        # Forecast
        last_smoothed = daily['smoothed'].iloc[-1]
        forecast_dates = pd.date_range(
            daily.index.max() + timedelta(days=1),
            periods=forecast_days
        )
        
        forecast = pd.DataFrame({
            'date': forecast_dates,
            'forecasted_quantity': [round(last_smoothed)] * forecast_days,
            'method': 'Exponential Smoothing'
        })
        
        return forecast


class StockoutAnalyzer:
    """Analyze stockout patterns and impact"""
    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self._load_data()
    
    def _load_data(self):
        """Load required datasets"""
        self.stockout_events = pd.read_csv(f"{self.data_dir}/stockout_events.csv",
                                           parse_dates=['stockout_start_date', 'stockout_end_date'])
        self.products = pd.read_csv(f"{self.data_dir}/products.csv")
        self.warehouses = pd.read_csv(f"{self.data_dir}/warehouses.csv")
    
    def analyze_stockout_impact(self) -> pd.DataFrame:
        """
        Analyze stockout frequency and financial impact.
        
        Returns:
            DataFrame with stockout analysis by product
        """
        stockouts = self.stockout_events.copy()
        stockouts['duration_days'] = (
            stockouts['stockout_end_date'] - stockouts['stockout_start_date']
        ).dt.days
        
        impact = stockouts.groupby(['warehouse_id', 'product_id']).agg({
            'stockout_id': 'count',
            'duration_days': 'mean',
            'demand_during_stockout': 'sum',
            'lost_sales_amount': 'sum'
        }).reset_index()
        impact.columns = ['warehouse_id', 'product_id', 'stockout_count',
                         'avg_duration_days', 'total_lost_units', 'total_lost_revenue']
        
        # Add product and warehouse info
        impact = impact.merge(
            self.products[['product_id', 'sku', 'product_name']],
            on='product_id'
        )
        impact = impact.merge(
            self.warehouses[['warehouse_id', 'warehouse_code']],
            on='warehouse_id'
        )
        
        # Calculate severity score
        impact['severity_score'] = (
            impact['stockout_count'] * 30 +
            impact['avg_duration_days'] * 10 +
            impact['total_lost_revenue'] / 100
        )
        
        # Round values
        impact['avg_duration_days'] = impact['avg_duration_days'].round(1)
        impact['total_lost_revenue'] = impact['total_lost_revenue'].round(2)
        impact['severity_score'] = impact['severity_score'].round(0)
        
        return impact[['warehouse_code', 'sku', 'product_name', 'stockout_count',
                       'avg_duration_days', 'total_lost_units', 'total_lost_revenue',
                       'severity_score']].sort_values('severity_score', ascending=False)
    
    def get_root_cause_analysis(self) -> pd.DataFrame:
        """
        Analyze stockout root causes.
        
        Returns:
            DataFrame with root cause distribution
        """
        cause_analysis = self.stockout_events.groupby('root_cause').agg({
            'stockout_id': 'count',
            'lost_sales_amount': 'sum',
            'demand_during_stockout': 'sum'
        }).reset_index()
        cause_analysis.columns = ['root_cause', 'occurrence_count', 
                                  'total_lost_revenue', 'total_lost_units']
        
        # Calculate percentage
        total_events = cause_analysis['occurrence_count'].sum()
        cause_analysis['pct_of_stockouts'] = (
            cause_analysis['occurrence_count'] / total_events * 100
        ).round(2)
        
        cause_analysis['total_lost_revenue'] = cause_analysis['total_lost_revenue'].round(2)
        
        return cause_analysis.sort_values('occurrence_count', ascending=False)


if __name__ == "__main__":
    # Example usage
    print("="*60)
    print("SUPPLY CHAIN ANALYTICS ENGINE")
    print("="*60)
    
    # Initialize analytics
    inv_analytics = InventoryAnalytics()
    supplier_analytics = SupplierAnalytics()
    stockout_analyzer = StockoutAnalyzer()
    
    # Run analyses
    print("\n1. ABC Classification")
    print("-"*40)
    abc = inv_analytics.calculate_abc_classification()
    print(abc.groupby('abc_class').agg({
        'product_id': 'count',
        'total_revenue': 'sum'
    }))
    
    print("\n2. Top Suppliers by Reliability")
    print("-"*40)
    supplier_scores = supplier_analytics.calculate_supplier_scores()
    print(supplier_scores.head(10)[['supplier_name', 'reliability_score', 'tier']])
    
    print("\n3. Stockout Root Causes")
    print("-"*40)
    root_causes = stockout_analyzer.get_root_cause_analysis()
    print(root_causes)
    
    print("\n4. Inventory Carrying Costs Summary")
    print("-"*40)
    carrying = inv_analytics.calculate_carrying_costs()
    warehouse_total = carrying.groupby('warehouse_code')['total_carrying_cost'].sum()
    print(warehouse_total)
