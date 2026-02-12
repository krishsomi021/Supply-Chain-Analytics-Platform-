# Analytics Platform Evaluation: Scaling Supply Chain Analytics

## Executive Summary

This document evaluates enterprise analytics platforms to scale the supply chain analytics solution from a prototype (PostgreSQL + Streamlit) to production-ready infrastructure supporting larger data volumes, more users, and enhanced visualization capabilities.

**Recommendation:** Migrate to **Snowflake** for data warehousing and **Tableau** for enterprise visualization, while retaining Streamlit for rapid prototyping and internal tools.

---

## Current State Assessment

### Existing Architecture
| Component | Technology | Limitations |
|-----------|------------|-------------|
| Database | PostgreSQL | Single-node, manual scaling, limited concurrency |
| Analytics | Python/Pandas | Memory-bound, single-machine processing |
| Visualization | Streamlit | Developer-focused, limited sharing, no governance |
| Data Volume | ~228K transactions | Sufficient for prototype, not for enterprise scale |

### Projected Scale Requirements
| Metric | Current | 12-Month Projection |
|--------|---------|---------------------|
| Transaction Volume | 228K rows | 10M+ rows |
| Concurrent Users | 1-5 | 50-100 |
| Dashboard Refresh | Manual | Real-time / 15-min intervals |
| Data Sources | CSV files | ERP, WMS, TMS, EDI feeds |

---

## Data Warehouse Evaluation: Snowflake vs. Alternatives

### Options Considered
1. **Snowflake** - Cloud-native data warehouse
2. **AWS Redshift** - AWS-native columnar warehouse
3. **Google BigQuery** - Serverless analytics
4. **PostgreSQL (scaled)** - Self-managed with read replicas

### Evaluation Criteria

| Criteria | Weight | Snowflake | Redshift | BigQuery | PostgreSQL |
|----------|--------|-----------|----------|----------|------------|
| Query Performance | 25% | 9 | 8 | 9 | 6 |
| Scalability | 20% | 10 | 8 | 10 | 5 |
| Cost Efficiency | 20% | 7 | 7 | 8 | 9 |
| Ease of Use | 15% | 9 | 7 | 8 | 8 |
| Ecosystem Integration | 10% | 9 | 8 | 7 | 7 |
| Maintenance Overhead | 10% | 10 | 6 | 10 | 4 |
| **Weighted Score** | 100% | **8.7** | 7.4 | 8.5 | 6.4 |

### Snowflake Advantages for Supply Chain Analytics

1. **Separation of Storage and Compute**
   - Scale compute for month-end reporting without paying for idle capacity
   - Run heavy ABC classification queries without impacting dashboard users

2. **Time Travel & Zero-Copy Cloning**
   - Query historical inventory snapshots without maintaining separate tables
   - Clone production data for testing reorder point algorithms

3. **Semi-Structured Data Support**
   - Native JSON handling for EDI/API integrations
   - Flatten nested supplier responses without ETL

4. **Automatic Clustering**
   - Self-optimizing for date-based queries (common in supply chain)
   - No manual index maintenance

5. **Data Sharing**
   - Share supplier scorecards directly with vendor portals
   - Receive customer demand signals without file transfers

### Proposed Snowflake Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     SNOWFLAKE ACCOUNT                           │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐ │
│  │   RAW LAYER     │  │  STAGING LAYER  │  │ ANALYTICS LAYER │ │
│  │   (Bronze)      │  │   (Silver)      │  │    (Gold)       │ │
│  ├─────────────────┤  ├─────────────────┤  ├─────────────────┤ │
│  │ • ERP extracts  │  │ • Cleaned data  │  │ • Star schema   │ │
│  │ • EDI files     │  │ • Deduped       │  │ • Fact tables   │ │
│  │ • API responses │  │ • Validated     │  │ • Aggregates    │ │
│  │ • IoT streams   │  │ • Conformed     │  │ • KPI views     │ │
│  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘ │
│           │                    │                    │          │
│           ▼                    ▼                    ▼          │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                    VIRTUAL WAREHOUSES                       ││
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   ││
│  │  │ ETL_WH   │  │REPORT_WH │  │ADHOC_WH  │  │ ML_WH    │   ││
│  │  │ (X-Small)│  │ (Medium) │  │ (Small)  │  │ (Large)  │   ││
│  │  │ Scheduled│  │ Tableau  │  │ Analysts │  │ Training │   ││
│  │  └──────────┘  └──────────┘  └──────────┘  └──────────┘   ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
```

### Cost Projection (Snowflake)

| Workload | Warehouse Size | Daily Hours | Monthly Cost |
|----------|---------------|-------------|--------------|
| ETL Processing | X-Small | 2 | $60 |
| Dashboard Queries | Small | 8 | $480 |
| Ad-hoc Analysis | Small | 4 | $240 |
| ML Training | Medium | 2 | $240 |
| Storage (1TB) | - | - | $23 |
| **Total** | | | **~$1,043/month** |

---

## Visualization Evaluation: Tableau vs. Alternatives

### Options Considered
1. **Tableau** - Enterprise BI platform
2. **Power BI** - Microsoft ecosystem
3. **Looker** - Google Cloud native
4. **Streamlit** - Python-native (current)

### Evaluation Criteria

| Criteria | Weight | Tableau | Power BI | Looker | Streamlit |
|----------|--------|---------|----------|--------|-----------|
| Visualization Quality | 25% | 10 | 8 | 8 | 6 |
| Self-Service Analytics | 20% | 9 | 9 | 7 | 4 |
| Enterprise Governance | 15% | 9 | 9 | 9 | 3 |
| Snowflake Integration | 15% | 10 | 7 | 8 | 6 |
| Learning Curve | 10% | 7 | 8 | 6 | 9 |
| Cost | 10% | 6 | 8 | 6 | 10 |
| Mobile Support | 5% | 9 | 8 | 7 | 5 |
| **Weighted Score** | 100% | **8.7** | 8.2 | 7.4 | 5.5 |

### Tableau Advantages for Supply Chain Dashboards

1. **Native Snowflake Connector**
   - Live connection with query pushdown
   - Leverage Snowflake compute for aggregations
   - No data extraction needed

2. **Advanced Geospatial**
   - Map warehouse locations and shipping routes
   - Regional demand heat maps
   - Supplier location analysis

3. **Sophisticated Calculations**
   - Table calculations for running totals (cumulative ABC)
   - Level of Detail (LOD) expressions for complex aggregations
   - Forecasting with built-in models

4. **Enterprise Features**
   - Row-level security for supplier portals
   - Certified data sources
   - Usage analytics and governance

5. **Embedding & Integration**
   - Embed dashboards in internal portals
   - API access for automated screenshots
   - Slack/Teams alerting integration

### Proposed Dashboard Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    TABLEAU SERVER / CLOUD                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              EXECUTIVE SUMMARY DASHBOARD                 │   │
│  │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐       │   │
│  │  │ Revenue │ │  Fill   │ │Stockout │ │Inventory│       │   │
│  │  │   KPI   │ │  Rate   │ │  Rate   │ │ Value   │       │   │
│  │  └─────────┘ └─────────┘ └─────────┘ └─────────┘       │   │
│  │  ┌───────────────────────────────────────────────┐     │   │
│  │  │           Revenue Trend (12 months)            │     │   │
│  │  └───────────────────────────────────────────────┘     │   │
│  │  ┌──────────────────┐  ┌──────────────────────────┐   │   │
│  │  │ ABC Distribution │  │  Warehouse Performance   │   │   │
│  │  └──────────────────┘  └──────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────┐  ┌─────────────────────────────────┐ │
│  │ INVENTORY HEALTH    │  │ SUPPLIER SCORECARD              │ │
│  │ • Stock levels      │  │ • Reliability matrix            │ │
│  │ • Reorder alerts    │  │ • Lead time trends              │ │
│  │ • Dead stock        │  │ • Tier distribution             │ │
│  │ • Turnover analysis │  │ • Performance vs. spend         │ │
│  └─────────────────────┘  └─────────────────────────────────┘ │
│                                                                 │
│  ┌─────────────────────┐  ┌─────────────────────────────────┐ │
│  │ DEMAND ANALYTICS    │  │ OPERATIONS DEEP-DIVE            │ │
│  │ • Forecast vs actual│  │ • EOQ recommendations           │ │
│  │ • Seasonality       │  │ • Safety stock optimization     │ │
│  │ • Regional demand   │  │ • Carrying cost analysis        │ │
│  └─────────────────────┘  └─────────────────────────────────┘ │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Streamlit Retention Strategy

Keep Streamlit for:
- **Rapid prototyping** of new analytics
- **ML model testing** interfaces
- **Internal tools** for data engineering
- **Cost-sensitive** departmental dashboards

---

## Implementation Roadmap

### Phase 1: Foundation (Weeks 1-4)
- [ ] Set up Snowflake account and configure warehouses
- [ ] Design and implement star schema (see `snowflake_schema.sql`)
- [ ] Build ETL pipeline from PostgreSQL to Snowflake
- [ ] Configure Tableau Server/Cloud connection

### Phase 2: Core Dashboards (Weeks 5-8)
- [ ] Migrate executive summary dashboard to Tableau
- [ ] Build inventory health workbook
- [ ] Create supplier scorecard with drill-down
- [ ] Implement row-level security

### Phase 3: Advanced Analytics (Weeks 9-12)
- [ ] Add demand forecasting with Tableau's built-in models
- [ ] Build geographic analysis (warehouse/supplier maps)
- [ ] Implement alerting for reorder points
- [ ] Create mobile-optimized views

### Phase 4: Optimization (Ongoing)
- [ ] Monitor query performance and optimize
- [ ] Implement data quality checks
- [ ] Train business users on self-service
- [ ] Document data lineage

---

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Cost overrun (Snowflake compute) | Medium | High | Implement resource monitors, auto-suspend |
| User adoption resistance | Medium | Medium | Phased rollout, training program |
| Data quality issues during migration | Medium | High | Validation framework, parallel running |
| Vendor lock-in | Low | Medium | Document transformations, maintain export capability |

---

## Success Metrics

| Metric | Current | Target (6 months) |
|--------|---------|-------------------|
| Dashboard load time | 5-10 sec | < 2 sec |
| Concurrent users supported | 5 | 50+ |
| Report refresh frequency | Manual | Every 15 min |
| Self-service adoption | 0% | 40% of queries |
| Data latency | 24 hours | 15 minutes |

---

## Appendix: File Inventory

| File | Description |
|------|-------------|
| `snowflake_schema.sql` | Complete star schema DDL for Snowflake |
| `tableau_export.py` | Python script to generate Tableau-ready extracts |
| `tableau_extracts/` | Pre-built CSV files for Tableau connection |

---

## Conclusion

The combination of **Snowflake** for scalable data warehousing and **Tableau** for enterprise visualization provides the optimal path to scale supply chain analytics. The star schema design enables efficient querying, while Tableau's native Snowflake connector ensures performance at scale.

Estimated total investment: **~$2,500/month** (Snowflake + Tableau Creator licenses)
Expected ROI: **3-6 months** through reduced manual reporting and faster decision-making

---

*Document prepared as part of Supply Chain Analytics Platform evaluation*
*Last updated: January 2026*
