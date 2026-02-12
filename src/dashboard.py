"""
Supply Chain Analytics Dashboard
Author: Krishna Somisetty
Description: Interactive Streamlit dashboard for supply chain optimization insights
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
import sys

# Add src to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.analytics import (
    InventoryAnalytics, 
    SupplierAnalytics, 
    StockoutAnalyzer,
    DemandForecaster
)

# Page config
st.set_page_config(
    page_title="Supply Chain Analytics",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        text-align: center;
    }
    .stMetric {
        background-color: #ffffff;
        padding: 15px;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
</style>
""", unsafe_allow_html=True)


@st.cache_data
def load_data(data_dir: str = "data"):
    """Load all datasets with caching"""
    data = {}
    files = [
        'warehouses', 'products', 'suppliers', 'customers',
        'purchase_orders', 'purchase_order_items',
        'sales_orders', 'sales_order_items',
        'inventory', 'inventory_transactions',
        'stockout_events', 'demand_forecasts', 'product_categories'
    ]
    
    for file in files:
        filepath = f"{data_dir}/{file}.csv"
        if os.path.exists(filepath):
            data[file] = pd.read_csv(filepath)
    
    # Parse dates
    if 'sales_orders' in data:
        data['sales_orders']['order_date'] = pd.to_datetime(data['sales_orders']['order_date'])
    if 'purchase_orders' in data:
        for col in ['order_date', 'expected_delivery_date', 'actual_delivery_date']:
            data['purchase_orders'][col] = pd.to_datetime(data['purchase_orders'][col])
    if 'stockout_events' in data:
        data['stockout_events']['stockout_start_date'] = pd.to_datetime(data['stockout_events']['stockout_start_date'])
        data['stockout_events']['stockout_end_date'] = pd.to_datetime(data['stockout_events']['stockout_end_date'])
    
    return data


@st.cache_resource
def get_analytics(data_dir: str = "data"):
    """Initialize analytics engines with caching"""
    return {
        'inventory': InventoryAnalytics(data_dir),
        'supplier': SupplierAnalytics(data_dir),
        'stockout': StockoutAnalyzer(data_dir),
        'forecaster': DemandForecaster(data_dir)
    }


def render_kpis(data: dict):
    """Render top-level KPI metrics"""
    st.subheader("📊 Key Performance Indicators")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    # Total inventory value
    inv = data['inventory'].merge(
        data['products'][['product_id', 'unit_cost']], 
        on='product_id'
    )
    total_inv_value = (inv['quantity_on_hand'] * inv['unit_cost']).sum()
    
    # Total stockout losses
    stockout_losses = data['stockout_events']['lost_sales_amount'].sum()
    
    # Fill rate
    po_items = data['purchase_order_items']
    fill_rate = po_items['quantity_received'].sum() / po_items['quantity_ordered'].sum() * 100
    
    # Active suppliers
    active_suppliers = len(data['suppliers'][data['suppliers']['is_active'] == True])
    
    # Products at risk (below reorder point)
    inv['available'] = inv['quantity_on_hand'] - inv['quantity_reserved']
    at_risk = len(inv[inv['available'] < inv['reorder_point']])
    
    with col1:
        st.metric("Total Inventory Value", f"${total_inv_value:,.0f}")
    with col2:
        st.metric("Stockout Losses (YTD)", f"${stockout_losses:,.0f}")
    with col3:
        st.metric("Average Fill Rate", f"{fill_rate:.1f}%")
    with col4:
        st.metric("Active Suppliers", active_suppliers)
    with col5:
        st.metric("Products at Risk", at_risk, delta=f"{at_risk} below ROP", delta_color="inverse")


def render_abc_analysis(analytics: dict):
    """Render ABC Classification analysis"""
    st.subheader("🏷️ ABC Classification Analysis")
    
    abc_data = analytics['inventory'].calculate_abc_classification()
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Summary by class
        summary = abc_data.groupby('abc_class').agg({
            'product_id': 'count',
            'total_revenue': 'sum'
        }).reset_index()
        summary.columns = ['Class', 'Products', 'Revenue']
        summary['Revenue %'] = summary['Revenue'] / summary['Revenue'].sum() * 100
        summary['Products %'] = summary['Products'] / summary['Products'].sum() * 100
        
        fig = make_subplots(rows=1, cols=2, specs=[[{"type": "pie"}, {"type": "pie"}]],
                           subplot_titles=("Revenue Distribution", "Product Distribution"))
        
        colors = {'A': '#2E86AB', 'B': '#A23B72', 'C': '#F18F01'}
        
        fig.add_trace(
            go.Pie(labels=summary['Class'], values=summary['Revenue'], 
                   marker_colors=[colors[c] for c in summary['Class']],
                   hole=0.4, name="Revenue"),
            row=1, col=1
        )
        
        fig.add_trace(
            go.Pie(labels=summary['Class'], values=summary['Products'],
                   marker_colors=[colors[c] for c in summary['Class']],
                   hole=0.4, name="Products"),
            row=1, col=2
        )
        
        fig.update_layout(height=350, showlegend=True)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Class metrics table
        st.markdown("**Class Summary**")
        summary_display = summary.copy()
        summary_display['Revenue'] = summary_display['Revenue'].apply(lambda x: f"${x:,.0f}")
        summary_display['Revenue %'] = summary_display['Revenue %'].apply(lambda x: f"{x:.1f}%")
        summary_display['Products %'] = summary_display['Products %'].apply(lambda x: f"{x:.1f}%")
        st.dataframe(summary_display, use_container_width=True, hide_index=True)
        
        st.markdown("""
        **Insights:**
        - **A-Class**: High-value items (80% revenue, ~20% products) - Focus on availability
        - **B-Class**: Medium-value items (15% revenue, ~30% products) - Balanced approach  
        - **C-Class**: Low-value items (5% revenue, ~50% products) - Minimize carrying cost
        """)
    
    # Top A-class products
    st.markdown("**Top A-Class Products by Revenue**")
    top_a = abc_data[abc_data['abc_class'] == 'A'].head(10)
    top_a['total_revenue'] = top_a['total_revenue'].apply(lambda x: f"${x:,.2f}")
    st.dataframe(top_a[['sku', 'product_name', 'quantity_ordered', 'total_revenue']], 
                 use_container_width=True, hide_index=True)


def render_inventory_turnover(analytics: dict):
    """Render inventory turnover analysis"""
    st.subheader("🔄 Inventory Turnover Analysis")
    
    turnover_data = analytics['inventory'].calculate_inventory_turnover()
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Turnover distribution
        category_counts = turnover_data['turnover_category'].value_counts()
        fig = px.bar(
            x=category_counts.index, 
            y=category_counts.values,
            color=category_counts.index,
            color_discrete_map={
                'Fast Moving': '#28a745',
                'Normal': '#17a2b8',
                'Slow Moving': '#ffc107',
                'Dead Stock': '#dc3545'
            },
            title="Products by Turnover Category"
        )
        fig.update_layout(showlegend=False, xaxis_title="Category", yaxis_title="Product Count")
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Turnover vs Inventory Value scatter
        sample = turnover_data.sample(min(200, len(turnover_data)))
        fig = px.scatter(
            sample,
            x='turnover_ratio',
            y='inventory_value',
            color='turnover_category',
            hover_data=['sku', 'product_name'],
            title="Turnover Ratio vs Inventory Value",
            color_discrete_map={
                'Fast Moving': '#28a745',
                'Normal': '#17a2b8',
                'Slow Moving': '#ffc107',
                'Dead Stock': '#dc3545'
            }
        )
        fig.update_layout(xaxis_title="Turnover Ratio", yaxis_title="Inventory Value ($)")
        st.plotly_chart(fig, use_container_width=True)
    
    # Metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        avg_turnover = turnover_data['turnover_ratio'].mean()
        st.metric("Average Turnover Ratio", f"{avg_turnover:.2f}")
    with col2:
        avg_dos = turnover_data[turnover_data['days_of_supply'] < np.inf]['days_of_supply'].mean()
        st.metric("Average Days of Supply", f"{avg_dos:.0f}")
    with col3:
        dead_stock = len(turnover_data[turnover_data['turnover_category'] == 'Dead Stock'])
        st.metric("Dead Stock Items", dead_stock)
    with col4:
        dead_value = turnover_data[turnover_data['turnover_category'] == 'Dead Stock']['inventory_value'].sum()
        st.metric("Dead Stock Value", f"${dead_value:,.0f}")
    
    # Dead stock details
    if st.checkbox("Show Dead Stock Details"):
        dead_items = turnover_data[turnover_data['turnover_category'] == 'Dead Stock']
        dead_items = dead_items.sort_values('inventory_value', ascending=False)
        dead_items['inventory_value'] = dead_items['inventory_value'].apply(lambda x: f"${x:,.2f}")
        st.dataframe(dead_items[['warehouse_code', 'sku', 'product_name', 'quantity_on_hand', 
                                 'inventory_value', 'days_of_supply']].head(20), 
                     use_container_width=True, hide_index=True)


def render_supplier_performance(analytics: dict):
    """Render supplier performance analysis"""
    st.subheader("🏭 Supplier Performance Analysis")
    
    supplier_scores = analytics['supplier'].calculate_supplier_scores()
    lead_time_data = analytics['supplier'].analyze_lead_time_variability()
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Tier distribution
        tier_counts = supplier_scores['tier'].value_counts()
        tier_order = ['Platinum', 'Gold', 'Silver', 'Bronze']
        tier_counts = tier_counts.reindex(tier_order, fill_value=0)
        
        fig = px.bar(
            x=tier_counts.index,
            y=tier_counts.values,
            color=tier_counts.index,
            color_discrete_map={
                'Platinum': '#C0C0C0',
                'Gold': '#FFD700',
                'Silver': '#A8A8A8',
                'Bronze': '#CD7F32'
            },
            title="Suppliers by Performance Tier"
        )
        fig.update_layout(showlegend=False, xaxis_title="Tier", yaxis_title="Supplier Count")
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # On-time vs Fill rate scatter
        fig = px.scatter(
            supplier_scores,
            x='on_time_rate',
            y='fill_rate',
            size='total_orders',
            color='tier',
            hover_data=['supplier_name', 'reliability_score'],
            title="Supplier Performance Matrix",
            color_discrete_map={
                'Platinum': '#C0C0C0',
                'Gold': '#FFD700',
                'Silver': '#A8A8A8',
                'Bronze': '#CD7F32'
            }
        )
        fig.add_hline(y=95, line_dash="dash", line_color="green", annotation_text="95% Fill Rate")
        fig.add_vline(x=90, line_dash="dash", line_color="green", annotation_text="90% On-Time")
        fig.update_layout(xaxis_title="On-Time Delivery %", yaxis_title="Fill Rate %")
        st.plotly_chart(fig, use_container_width=True)
    
    # Top and bottom performers
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**🏆 Top 5 Suppliers**")
        top5 = supplier_scores.nlargest(5, 'reliability_score')
        st.dataframe(top5[['supplier_name', 'on_time_rate', 'fill_rate', 'reliability_score', 'tier']], 
                     use_container_width=True, hide_index=True)
    
    with col2:
        st.markdown("**⚠️ Bottom 5 Suppliers**")
        bottom5 = supplier_scores.nsmallest(5, 'reliability_score')
        st.dataframe(bottom5[['supplier_name', 'on_time_rate', 'fill_rate', 'reliability_score', 'tier']], 
                     use_container_width=True, hide_index=True)
    
    # Lead time variability
    st.markdown("**Lead Time Variability by Supplier**")
    fig = px.bar(
        lead_time_data.sort_values('cv_pct'),
        x='supplier_name',
        y='cv_pct',
        color='reliability_category',
        title="Lead Time Coefficient of Variation (Lower is Better)",
        color_discrete_map={
            'Highly Reliable': '#28a745',
            'Reliable': '#17a2b8',
            'Variable': '#ffc107',
            'Unreliable': '#dc3545'
        }
    )
    fig.update_layout(xaxis_title="Supplier", yaxis_title="CV %", xaxis_tickangle=-45)
    st.plotly_chart(fig, use_container_width=True)


def render_stockout_analysis(analytics: dict, data: dict):
    """Render stockout analysis"""
    st.subheader("🚨 Stockout Analysis")
    
    stockout_impact = analytics['stockout'].analyze_stockout_impact()
    root_causes = analytics['stockout'].get_root_cause_analysis()
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Root cause distribution
        fig = px.pie(
            root_causes,
            values='occurrence_count',
            names='root_cause',
            title="Stockout Root Causes",
            hole=0.4
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Lost revenue by root cause
        fig = px.bar(
            root_causes.sort_values('total_lost_revenue', ascending=True),
            x='total_lost_revenue',
            y='root_cause',
            orientation='h',
            title="Lost Revenue by Root Cause",
            color='total_lost_revenue',
            color_continuous_scale='Reds'
        )
        fig.update_layout(xaxis_title="Lost Revenue ($)", yaxis_title="Root Cause", showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    
    # KPIs
    col1, col2, col3, col4 = st.columns(4)
    
    total_events = len(data['stockout_events'])
    total_lost = data['stockout_events']['lost_sales_amount'].sum()
    avg_duration = (data['stockout_events']['stockout_end_date'] - 
                   data['stockout_events']['stockout_start_date']).dt.days.mean()
    products_affected = data['stockout_events']['product_id'].nunique()
    
    with col1:
        st.metric("Total Stockout Events", f"{total_events:,}")
    with col2:
        st.metric("Total Lost Revenue", f"${total_lost:,.0f}")
    with col3:
        st.metric("Avg Stockout Duration", f"{avg_duration:.1f} days")
    with col4:
        st.metric("Products Affected", products_affected)
    
    # Worst affected products
    st.markdown("**Most Impacted Products by Stockouts**")
    worst = stockout_impact.head(10)
    worst['total_lost_revenue'] = worst['total_lost_revenue'].apply(lambda x: f"${x:,.2f}")
    st.dataframe(worst[['warehouse_code', 'sku', 'product_name', 'stockout_count', 
                        'avg_duration_days', 'total_lost_units', 'total_lost_revenue', 'severity_score']], 
                 use_container_width=True, hide_index=True)


def render_reorder_optimization(analytics: dict):
    """Render reorder point optimization"""
    st.subheader("📈 Reorder Point Optimization")
    
    service_level = st.slider("Target Service Level", 0.90, 0.99, 0.95, 0.01)
    
    rop_data = analytics['inventory'].calculate_reorder_points(service_level=service_level)
    
    # Summary by recommendation
    rec_summary = rop_data['recommendation'].value_counts()
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        increase = rec_summary.get('Increase ROP', 0)
        st.metric("Products to Increase ROP", increase, delta="Reduce stockout risk")
    with col2:
        decrease = rec_summary.get('Decrease ROP', 0)
        st.metric("Products to Decrease ROP", decrease, delta="Reduce carrying cost")
    with col3:
        optimal = rec_summary.get('Optimal', 0)
        st.metric("Optimal ROP", optimal)
    
    # Visualization
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.pie(
            values=rec_summary.values,
            names=rec_summary.index,
            title="ROP Recommendations",
            color=rec_summary.index,
            color_discrete_map={
                'Increase ROP': '#dc3545',
                'Decrease ROP': '#ffc107',
                'Optimal': '#28a745'
            }
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Current vs Calculated ROP scatter
        sample = rop_data.sample(min(100, len(rop_data)))
        fig = px.scatter(
            sample,
            x='current_rop',
            y='calculated_rop',
            color='recommendation',
            hover_data=['sku', 'product_name'],
            title="Current vs Calculated Reorder Points",
            color_discrete_map={
                'Increase ROP': '#dc3545',
                'Decrease ROP': '#ffc107',
                'Optimal': '#28a745'
            }
        )
        # Add diagonal line
        max_val = max(sample['current_rop'].max(), sample['calculated_rop'].max())
        fig.add_trace(go.Scatter(x=[0, max_val], y=[0, max_val], mode='lines', 
                                 name='Perfect Alignment', line=dict(dash='dash', color='gray')))
        fig.update_layout(xaxis_title="Current ROP", yaxis_title="Calculated ROP")
        st.plotly_chart(fig, use_container_width=True)
    
    # Products needing attention
    needs_increase = rop_data[rop_data['recommendation'] == 'Increase ROP']
    if len(needs_increase) > 0:
        st.markdown("**⚠️ Products Needing Higher Reorder Points (Top 10)**")
        display_cols = ['warehouse_code', 'sku', 'product_name', 'quantity_on_hand',
                       'avg_daily_demand', 'safety_stock', 'current_rop', 'calculated_rop']
        st.dataframe(needs_increase[display_cols].head(10), use_container_width=True, hide_index=True)


def render_eoq_analysis(analytics: dict):
    """Render EOQ analysis"""
    st.subheader("📦 Economic Order Quantity Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        ordering_cost = st.number_input("Ordering Cost per Order ($)", 25, 200, 50)
    with col2:
        holding_rate = st.slider("Annual Holding Cost Rate (%)", 15, 40, 25) / 100
    
    eoq_data = analytics['inventory'].calculate_eoq(
        ordering_cost=ordering_cost,
        holding_cost_rate=holding_rate
    )
    
    col1, col2 = st.columns(2)
    
    with col1:
        # EOQ distribution
        fig = px.histogram(
            eoq_data,
            x='eoq',
            nbins=50,
            title="Distribution of Economic Order Quantities",
            color_discrete_sequence=['#2E86AB']
        )
        fig.update_layout(xaxis_title="EOQ (units)", yaxis_title="Product Count")
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Orders per year distribution
        fig = px.histogram(
            eoq_data,
            x='orders_per_year',
            nbins=30,
            title="Distribution of Orders per Year",
            color_discrete_sequence=['#A23B72']
        )
        fig.update_layout(xaxis_title="Orders per Year", yaxis_title="Product Count")
        st.plotly_chart(fig, use_container_width=True)
    
    # Summary metrics
    total_annual_cost = eoq_data['annual_inventory_cost'].sum()
    avg_orders = eoq_data['orders_per_year'].mean()
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Optimal Inventory Cost", f"${total_annual_cost:,.0f}")
    with col2:
        st.metric("Average Orders per Product", f"{avg_orders:.1f}/year")
    with col3:
        avg_cycle = eoq_data['days_between_orders'].mean()
        st.metric("Average Order Cycle", f"{avg_cycle:.0f} days")
    
    # Top products by EOQ
    st.markdown("**Products with Highest EOQ Values**")
    top_eoq = eoq_data.nlargest(10, 'eoq')
    top_eoq['annual_inventory_cost'] = top_eoq['annual_inventory_cost'].apply(lambda x: f"${x:,.2f}")
    st.dataframe(top_eoq[['sku', 'product_name', 'annual_demand', 'eoq', 
                          'orders_per_year', 'days_between_orders', 'annual_inventory_cost']], 
                 use_container_width=True, hide_index=True)


def render_carrying_costs(analytics: dict):
    """Render carrying costs analysis"""
    st.subheader("💰 Inventory Carrying Costs")
    
    carrying_data = analytics['inventory'].calculate_carrying_costs()
    
    # Summary by warehouse
    warehouse_summary = carrying_data.groupby(['warehouse_code', 'warehouse_name']).agg({
        'product_count': 'sum',
        'total_units': 'sum',
        'inventory_value': 'sum',
        'total_carrying_cost': 'sum'
    }).reset_index()
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.bar(
            warehouse_summary,
            x='warehouse_code',
            y='total_carrying_cost',
            title="Annual Carrying Cost by Warehouse",
            color='warehouse_code',
            text=warehouse_summary['total_carrying_cost'].apply(lambda x: f"${x/1000:.0f}K")
        )
        fig.update_traces(textposition='outside')
        fig.update_layout(showlegend=False, xaxis_title="Warehouse", yaxis_title="Carrying Cost ($)")
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig = px.pie(
            warehouse_summary,
            values='inventory_value',
            names='warehouse_code',
            title="Inventory Value Distribution by Warehouse"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Cost breakdown
    st.markdown("**Cost Component Breakdown**")
    
    total_costs = carrying_data.agg({
        'capital_cost': 'sum',
        'storage_cost': 'sum',
        'insurance_cost': 'sum',
        'obsolescence_cost': 'sum',
        'handling_cost': 'sum'
    })
    
    cost_breakdown = pd.DataFrame({
        'Component': ['Capital Cost (8%)', 'Storage (5%)', 'Insurance (3%)', 
                     'Obsolescence (2%)', 'Handling (2%)'],
        'Amount': total_costs.values
    })
    
    fig = px.bar(
        cost_breakdown,
        x='Component',
        y='Amount',
        title="Carrying Cost Components",
        color='Component',
        text=cost_breakdown['Amount'].apply(lambda x: f"${x:,.0f}")
    )
    fig.update_traces(textposition='outside')
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True)
    
    # Summary metrics
    total_value = carrying_data['inventory_value'].sum()
    total_carrying = carrying_data['total_carrying_cost'].sum()
    monthly_carrying = carrying_data['monthly_carrying_cost'].sum()
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Inventory Value", f"${total_value:,.0f}")
    with col2:
        st.metric("Annual Carrying Cost", f"${total_carrying:,.0f}")
    with col3:
        st.metric("Monthly Carrying Cost", f"${monthly_carrying:,.0f}")


def main():
    """Main dashboard application"""
    
    st.title("📦 Supply Chain Optimization & Inventory Analytics")
    st.markdown("*Comprehensive analytics dashboard for supply chain management*")
    
    # Sidebar navigation
    st.sidebar.title("Navigation")
    page = st.sidebar.radio(
        "Select Analysis",
        [
            "📊 Executive Summary",
            "🏷️ ABC Classification",
            "🔄 Inventory Turnover",
            "🏭 Supplier Performance",
            "🚨 Stockout Analysis",
            "📈 Reorder Optimization",
            "📦 EOQ Analysis",
            "💰 Carrying Costs"
        ]
    )
    
    # Load data
    data_dir = "data"
    
    # Check if data exists
    if not os.path.exists(f"{data_dir}/products.csv"):
        st.error("Data not found! Please run the data generator first.")
        st.code("python src/data_generator.py", language="bash")
        return
    
    data = load_data(data_dir)
    analytics = get_analytics(data_dir)
    
    # Render selected page
    if page == "📊 Executive Summary":
        render_kpis(data)
        st.markdown("---")
        
        col1, col2 = st.columns(2)
        with col1:
            # Sales trend
            sales = data['sales_orders'].copy()
            sales['month'] = sales['order_date'].dt.to_period('M').astype(str)
            monthly_sales = sales.groupby('month')['total_amount'].sum().reset_index()
            
            fig = px.line(monthly_sales, x='month', y='total_amount', 
                         title="Monthly Sales Trend", markers=True)
            fig.update_layout(xaxis_title="Month", yaxis_title="Revenue ($)")
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Inventory by warehouse
            inv = data['inventory'].merge(data['products'][['product_id', 'unit_cost']], on='product_id')
            inv['value'] = inv['quantity_on_hand'] * inv['unit_cost']
            warehouse_inv = inv.groupby('warehouse_id')['value'].sum().reset_index()
            warehouse_inv = warehouse_inv.merge(data['warehouses'][['warehouse_id', 'warehouse_code']], on='warehouse_id')
            
            fig = px.bar(warehouse_inv, x='warehouse_code', y='value',
                        title="Inventory Value by Warehouse", color='warehouse_code')
            fig.update_layout(showlegend=False, xaxis_title="Warehouse", yaxis_title="Value ($)")
            st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("---")
        st.info("Use the sidebar to navigate to detailed analysis sections.")
        
    elif page == "🏷️ ABC Classification":
        render_abc_analysis(analytics)
        
    elif page == "🔄 Inventory Turnover":
        render_inventory_turnover(analytics)
        
    elif page == "🏭 Supplier Performance":
        render_supplier_performance(analytics)
        
    elif page == "🚨 Stockout Analysis":
        render_stockout_analysis(analytics, data)
        
    elif page == "📈 Reorder Optimization":
        render_reorder_optimization(analytics)
        
    elif page == "📦 EOQ Analysis":
        render_eoq_analysis(analytics)
        
    elif page == "💰 Carrying Costs":
        render_carrying_costs(analytics)
    
    # Footer
    st.sidebar.markdown("---")
    st.sidebar.markdown("**Supply Chain Analytics**")
    st.sidebar.markdown("Krishna Somisetty")
    st.sidebar.markdown("Built with Streamlit & Python")


if __name__ == "__main__":
    main()
