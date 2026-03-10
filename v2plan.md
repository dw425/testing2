# Blueprint IQ v2 — Complete Remodel Plan

> **Status:** Draft
> **Date:** 2026-03-10
> **Scope:** Remodel all verticals to align with Databricks Industry Outcome Maps, add 2 new verticals, expand data models, rename Healthcare → Health & Life Sciences

---

## 1. Vertical Lineup (v2)

| # | Vertical | Status | Icon | Data Volume |
|---|----------|--------|------|-------------|
| 1 | **Gaming (CMEG)** | Remodel | `fa-gamepad` | 15K rows |
| 2 | **Telecommunications** | NEW | `fa-tower-cell` | 15K rows |
| 3 | **Media & Entertainment** | NEW | `fa-film` | 15K rows |
| 4 | **Financial Services** | Major Expansion | `fa-building-columns` | 100K rows |
| 5 | **Health & Life Sciences** | Rename + Remodel | `fa-heart-pulse` | 15K rows |

**Dropped:** Manufacturing, Risk (not in outcome map lineup)

---

## 2. Gaming (CMEG) — Full Outcome Map Alignment

### 2.1 Business Objective Hierarchy

```
GAMING INDUSTRY
├── PILLAR 1: Next-Gen Player Experience
│   ├── Know Your Player
│   │   ├── Player 360 / Unified Profile
│   │   ├── Player Segmentation & Clustering
│   │   ├── Sentiment Analysis (reviews, social, in-game)
│   │   └── Behavioral Analytics (session patterns, progression)
│   ├── Grow & Retain Playerbase
│   │   ├── Churn Prediction & Intervention
│   │   ├── Retention Optimization (D1/D7/D30)
│   │   ├── Personalized Re-engagement Campaigns
│   │   ├── UA Campaign Attribution & ROAS
│   │   └── Cross-promotion & Discovery
│   └── Grow Revenue
│       ├── Dynamic Pricing & Offer Optimization
│       ├── Player LTV Prediction
│       ├── In-App Purchase Funnel Optimization
│       ├── Ad Monetization Optimization (ARPDAU, CPM, CPI)
│       └── Subscription & Battle Pass Analytics
│
├── PILLAR 2: Build Great Games
│   ├── De-risk Game Development
│   │   ├── Milestone Completion Rate Tracking
│   │   ├── Build Time Optimization
│   │   ├── Playtesting Analytics & Feature Impact
│   │   ├── Pre-launch Audience Sizing
│   │   └── Competitive Intelligence
│   └── Effective Live Operations
│       ├── Real-time Event Performance
│       ├── Content ROI Analysis
│       ├── Bug Resolution Rate & Quality Metrics
│       ├── User Review Score Monitoring
│       ├── Economy Health & Inflation Index
│       └── Season/Battle Pass Engagement
│
└── PILLAR 3: Efficient Operations
    ├── Optimize Operations
    │   ├── Server Uptime & SLA Compliance
    │   ├── MTTR (Mean Time to Recovery)
    │   ├── Infrastructure Cost Optimization
    │   ├── CDN & Latency Monitoring
    │   └── Auto-scaling Efficiency
    └── Democratize Data
        ├── Self-service Analytics Adoption
        ├── Time to Insight
        ├── Data Quality Score
        ├── Data Security & Compliance Risk
        └── Cross-game Data Unification
```

### 2.2 Key Metrics (Dashboard Blocks)

| KPI | Category | Target/Benchmark |
|-----|----------|-----------------|
| Player LTV (avg) | Revenue | $47.80 |
| D1 / D7 / D30 Retention | Retention | 68% / 41% / 22% |
| ROAS (Return on Ad Spend) | UA/Marketing | > 1.5x |
| ARPDAU | Revenue | $0.118 |
| CPM / CPI | UA/Marketing | $8.50 / $2.30 |
| CLV (Customer Lifetime Value) | Revenue | $52.40 |
| Churn Risk (High) | Retention | 34.2K players |
| Milestone Completion Rate | Development | 87% |
| Bug Resolution Rate | Quality | 94% within SLA |
| Build Time (avg) | Development | 4.2 hours |
| User Review Score | Quality | 4.3 / 5.0 |
| Server Uptime | Operations | 99.95% |
| MTTR | Operations | 12.4 min |
| Content ROI | Live Ops | 3.2x |
| CAC (Customer Acquisition Cost) | UA/Marketing | $3.80 |
| Time to Insight | Data Platform | < 5 min |
| Data Security Risk Score | Compliance | Low |

### 2.3 Dashboard Pages (v2)

1. **Live Ops Command Center** — Real-time KPIs: DAU, Concurrent, ARPDAU, Server Uptime, MTTR
2. **Player Intelligence** — Player 360, segmentation, retention curves (D1/D7/D30), churn heatmap, LTV distribution
3. **Revenue & Monetization** — Revenue by title, ARPDAU trends, IAP funnel, ad revenue, battle pass engagement, CLV
4. **User Acquisition & Growth** — ROAS by campaign, CPI/CPM, CAC, attribution, re-engagement ROI
5. **Game Development & Quality** — Milestone tracker, build times, bug resolution rate, review scores, content ROI
6. **Infrastructure & Ops** — Server uptime by region, MTTR, latency heatmap, cost optimization, auto-scaling

### 2.4 Data Model

**Table: `gaming_iq.gold.player_metrics`** (~5K rows)
| Column | Type | Description |
|--------|------|-------------|
| player_id | STRING | Unique player identifier |
| game_title | STRING | Stellar Conquest / Shadow Realms / Velocity Rush |
| segment | STRING | Whale / Dolphin / Minnow / Free-to-Play |
| region | STRING | NA-East / NA-West / EU-West / EU-North / APAC-SEA / APAC-JP |
| ltv | DOUBLE | Lifetime value ($) |
| clv | DOUBLE | Customer lifetime value ($) |
| d1_retained | BOOLEAN | Day 1 retention flag |
| d7_retained | BOOLEAN | Day 7 retention flag |
| d30_retained | BOOLEAN | Day 30 retention flag |
| churn_risk_score | DOUBLE | 0.0-1.0 churn probability |
| avg_session_minutes | DOUBLE | Average session length |
| sessions_7d | INT | Sessions in last 7 days |
| total_spend | DOUBLE | Total in-app purchases |
| acquisition_source | STRING | Organic / Paid / Social / Cross-promo |
| acquisition_cost | DOUBLE | CAC for this player |
| signup_date | DATE | Account creation date |
| last_login | TIMESTAMP | Last activity |

**Table: `gaming_iq.gold.revenue_metrics`** (~3K rows)
| Column | Type | Description |
|--------|------|-------------|
| date | DATE | Metric date |
| game_title | STRING | Game title |
| region | STRING | Region |
| daily_revenue | DOUBLE | Revenue for the day |
| arpdau | DOUBLE | Avg revenue per DAU |
| iap_revenue | DOUBLE | In-app purchase revenue |
| ad_revenue | DOUBLE | Ad monetization revenue |
| subscription_revenue | DOUBLE | Battle pass / subscription |
| cpm | DOUBLE | Cost per mille (ads) |
| cpi | DOUBLE | Cost per install |
| roas | DOUBLE | Return on ad spend |
| dau | INT | Daily active users |
| concurrent_peak | INT | Peak concurrent |

**Table: `gaming_iq.gold.ua_campaigns`** (~2K rows)
| Column | Type | Description |
|--------|------|-------------|
| campaign_id | STRING | Campaign identifier |
| game_title | STRING | Game title |
| channel | STRING | Google / Meta / TikTok / Apple / Organic |
| spend | DOUBLE | Campaign spend |
| installs | INT | Installs driven |
| cpi | DOUBLE | Cost per install |
| roas_d7 | DOUBLE | 7-day ROAS |
| roas_d30 | DOUBLE | 30-day ROAS |
| retention_d7 | DOUBLE | D7 retention of acquired cohort |
| start_date | DATE | Campaign start |
| end_date | DATE | Campaign end |

**Table: `gaming_iq.gold.game_development`** (~1K rows)
| Column | Type | Description |
|--------|------|-------------|
| game_title | STRING | Game title |
| milestone | STRING | Alpha / Beta / RC / Launch / Patch |
| milestone_completion_pct | DOUBLE | Completion rate |
| avg_build_time_hours | DOUBLE | Build time |
| bug_count_open | INT | Open bugs |
| bug_resolution_rate | DOUBLE | % resolved within SLA |
| review_score | DOUBLE | Avg user review (1-5) |
| review_count | INT | Number of reviews |
| content_update | STRING | Season/event name |
| content_roi | DOUBLE | Revenue lift multiplier |
| date | DATE | Record date |

**Table: `gaming_iq.gold.infrastructure_ops`** (~2K rows)
| Column | Type | Description |
|--------|------|-------------|
| region | STRING | Server region |
| game_title | STRING | Game title |
| server_uptime_pct | DOUBLE | Uptime percentage |
| mttr_minutes | DOUBLE | Mean time to recovery |
| avg_latency_ms | DOUBLE | Average latency |
| p99_latency_ms | DOUBLE | 99th percentile latency |
| infra_cost_daily | DOUBLE | Daily infrastructure cost |
| autoscale_efficiency | DOUBLE | Scaling efficiency score |
| incidents_24h | INT | Incident count |
| date | DATE | Record date |

**Table: `gaming_iq.gold.live_ops_events`** (~2K rows)
| Column | Type | Description |
|--------|------|-------------|
| event_id | STRING | Event identifier |
| game_title | STRING | Game title |
| event_name | STRING | Season/event name |
| event_type | STRING | Season / Limited / Competitive / Collab |
| participation_rate | DOUBLE | % of DAU participating |
| revenue_impact | DOUBLE | Revenue lift |
| engagement_lift | DOUBLE | Session length increase |
| start_date | DATE | Event start |
| end_date | DATE | Event end |

---

## 3. Telecommunications — NEW Vertical

### 3.1 Business Objective Hierarchy

```
TELECOMMUNICATIONS
├── PILLAR 1: Improve Customer Experience
│   ├── Consumer CX
│   │   ├── Customer 360 / Unified Profile
│   │   ├── Churn Prediction & Retention
│   │   ├── Personalized Offers & Next-Best-Action
│   │   ├── Digital Channel Optimization
│   │   └── Customer Sentiment (NPS, CSAT)
│   ├── SMB & Enterprise CX
│   │   ├── Account Health Scoring
│   │   ├── Upsell / Cross-sell Propensity
│   │   ├── SLA Compliance Monitoring
│   │   └── Contract Renewal Prediction
│   └── Service Delivery
│       ├── Order-to-Activate Cycle Time
│       ├── First-Call Resolution Rate
│       ├── Truck Roll Optimization
│       └── Self-service Adoption Rate
│
├── PILLAR 2: Optimize & Automate Operations
│   ├── Network Operations
│   │   ├── Network Uptime & Availability
│   │   ├── Predictive Maintenance (cell towers, fiber)
│   │   ├── Capacity Planning & Utilization
│   │   ├── Anomaly Detection (outages, degradation)
│   │   └── 5G Rollout Progress Tracking
│   └── Field Operations
│       ├── Technician Dispatch Optimization
│       ├── Mean Time to Repair (MTTR)
│       ├── Parts Inventory Optimization
│       ├── Work Order Completion Rate
│       └── SLA Achievement Rate
│
├── PILLAR 3: Reduce Fraud & Risk
│   ├── Fraud Prevention
│   │   ├── Subscription Fraud Detection
│   │   ├── SIM Swap Fraud Detection
│   │   ├── International Revenue Share Fraud (IRSF)
│   │   └── Device Fraud Detection
│   ├── Compliance & Regulatory
│   │   ├── CPNI Compliance Monitoring
│   │   ├── Regulatory Reporting Automation
│   │   └── Data Privacy (GDPR/CCPA) Compliance
│   └── Cybersecurity
│       ├── DDoS Detection & Mitigation
│       ├── Network Intrusion Detection
│       └── Threat Intelligence Scoring
│
└── PILLAR 4: Monetize Beyond Connectivity
    ├── B2B Digital Products
    │   ├── IoT Platform Revenue
    │   ├── Edge Computing Services
    │   ├── Private 5G / Network Slicing
    │   └── API Monetization
    └── Data Monetization
        ├── Location Intelligence Products
        ├── Audience Insights (anonymized)
        └── Partner Ecosystem Revenue
```

### 3.2 Key Metrics (Dashboard Blocks)

| KPI | Category | Target/Benchmark |
|-----|----------|-----------------|
| Customer Churn Rate | Consumer CX | 1.8% monthly |
| NPS (Net Promoter Score) | Consumer CX | 42 |
| CSAT Score | Consumer CX | 4.1 / 5.0 |
| ARPU (Avg Revenue Per User) | Revenue | $52.30 |
| Network Uptime | Network Ops | 99.97% |
| MTTR (Mean Time to Repair) | Field Ops | 3.2 hours |
| First-Call Resolution Rate | Service Delivery | 78% |
| Order-to-Activate (days) | Service Delivery | 2.4 days |
| Fraud Detection Rate | Fraud | 96.2% |
| Fraud Loss Prevented ($) | Fraud | $4.2M/month |
| SLA Achievement Rate | Operations | 97.8% |
| 5G Coverage (%) | Network | 67% metro areas |
| IoT Connected Devices | B2B Products | 2.4M |
| Subscriber Base | Revenue | 14.2M |
| Revenue per Site | Network | $8,400/month |
| Truck Roll Rate | Field Ops | 32% of tickets |
| Self-service Adoption | Digital | 61% |

### 3.3 Dashboard Pages (v2)

1. **Network Command Center** — Uptime, MTTR, 5G rollout, capacity utilization, outage map
2. **Customer Intelligence** — Churn risk, NPS/CSAT trends, Customer 360 drill-down, next-best-action
3. **Revenue & Growth** — ARPU trends, subscriber growth, plan mix, upsell/cross-sell, contract renewals
4. **Fraud & Security** — Fraud detection rate, SIM swap alerts, IRSF monitoring, threat intelligence
5. **Field Operations** — Dispatch optimization, work order completion, truck roll rate, parts inventory
6. **B2B & IoT** — IoT device growth, edge computing revenue, network slicing utilization, API usage

### 3.4 Data Model

**Table: `telecom_iq.gold.subscriber_metrics`** (~5K rows)
| Column | Type | Description |
|--------|------|-------------|
| subscriber_id | STRING | Unique subscriber ID |
| segment | STRING | Consumer / SMB / Enterprise |
| plan_type | STRING | Prepaid / Postpaid / Family / Business |
| region | STRING | Northeast / Southeast / Midwest / West / International |
| arpu | DOUBLE | Avg revenue per user |
| tenure_months | INT | Customer tenure |
| churn_risk_score | DOUBLE | 0.0-1.0 churn probability |
| nps_score | INT | Last NPS survey score |
| csat_score | DOUBLE | Last CSAT score |
| data_usage_gb | DOUBLE | Monthly data usage |
| call_minutes | INT | Monthly call minutes |
| support_tickets_90d | INT | Support tickets last 90 days |
| contract_end_date | DATE | Contract end date |
| upsell_propensity | DOUBLE | Upsell likelihood score |
| last_interaction | TIMESTAMP | Last touchpoint |

**Table: `telecom_iq.gold.network_performance`** (~3K rows)
| Column | Type | Description |
|--------|------|-------------|
| site_id | STRING | Cell site / node ID |
| region | STRING | Region |
| technology | STRING | 4G LTE / 5G / Fiber / Fixed Wireless |
| uptime_pct | DOUBLE | Uptime percentage |
| avg_latency_ms | DOUBLE | Average latency |
| capacity_utilization_pct | DOUBLE | Capacity used |
| connected_devices | INT | Active connections |
| throughput_mbps | DOUBLE | Avg throughput |
| outage_incidents_30d | INT | Outages last 30 days |
| mttr_hours | DOUBLE | Mean time to repair |
| revenue_per_site | DOUBLE | Revenue generated |
| date | DATE | Record date |

**Table: `telecom_iq.gold.fraud_events`** (~2K rows)
| Column | Type | Description |
|--------|------|-------------|
| event_id | STRING | Fraud event ID |
| fraud_type | STRING | SIM Swap / Subscription / IRSF / Device |
| severity | STRING | Low / Medium / High / Critical |
| detected_at | TIMESTAMP | Detection time |
| subscriber_id | STRING | Affected subscriber |
| amount_at_risk | DOUBLE | Potential loss |
| blocked | BOOLEAN | Was it blocked |
| detection_method | STRING | ML Model / Rule / Manual |
| region | STRING | Region |
| resolution_status | STRING | Open / Investigating / Resolved |

**Table: `telecom_iq.gold.field_operations`** (~3K rows)
| Column | Type | Description |
|--------|------|-------------|
| work_order_id | STRING | Work order ID |
| order_type | STRING | Install / Repair / Maintenance / Upgrade |
| region | STRING | Region |
| technician_id | STRING | Assigned technician |
| sla_target_hours | DOUBLE | SLA target |
| actual_hours | DOUBLE | Actual completion time |
| sla_met | BOOLEAN | SLA achieved |
| truck_roll_required | BOOLEAN | Truck roll needed |
| first_time_fix | BOOLEAN | Fixed on first visit |
| parts_used | INT | Parts consumed |
| customer_satisfaction | DOUBLE | Post-service CSAT |
| date | DATE | Completion date |

**Table: `telecom_iq.gold.b2b_iot_metrics`** (~2K rows)
| Column | Type | Description |
|--------|------|-------------|
| product_id | STRING | Product identifier |
| product_type | STRING | IoT Platform / Edge Computing / Private 5G / API |
| customer_segment | STRING | SMB / Enterprise / Government |
| connected_devices | INT | Active IoT devices |
| monthly_revenue | DOUBLE | Monthly recurring revenue |
| api_calls_monthly | BIGINT | API call volume |
| data_processed_tb | DOUBLE | Data processed |
| uptime_pct | DOUBLE | Service uptime |
| region | STRING | Region |
| date | DATE | Record date |

---

## 4. Media & Entertainment — NEW Vertical

### 4.1 Business Objective Hierarchy

```
MEDIA & ENTERTAINMENT
├── PILLAR 1: Publishers & Streaming
│   ├── Audience Intelligence
│   │   ├── Audience 360 / Unified Viewer Profile
│   │   ├── Audience Segmentation & Lookalikes
│   │   ├── Content Affinity Modeling
│   │   ├── Cross-platform Identity Resolution
│   │   └── Sentiment & Social Listening
│   ├── Content Strategy & Performance
│   │   ├── Content ROI Analysis
│   │   ├── Recommendation Engine Effectiveness
│   │   ├── Catalog Utilization & Freshness
│   │   ├── Licensing & Acquisition Optimization
│   │   └── Original vs Licensed Performance
│   └── Monetization & Revenue
│       ├── Subscription LTV & Churn
│       ├── Ad Yield Optimization (CPM, Fill Rate)
│       ├── AVOD / SVOD / TVOD Mix Optimization
│       ├── Commerce & Merchandising Revenue
│       ├── Ticketing & Live Events Revenue
│       └── Paywall Optimization
│
├── PILLAR 2: Agencies & Advertisers
│   ├── Know Your Audience
│   │   ├── Audience Data Clean Rooms
│   │   ├── First-Party Data Activation
│   │   ├── Privacy-safe Measurement
│   │   └── Audience Overlap & Reach Analysis
│   ├── Activate & Measure
│   │   ├── Campaign Performance (ROAS, CTR, VCR)
│   │   ├── Multi-touch Attribution
│   │   ├── Media Mix Modeling
│   │   ├── Programmatic Yield Optimization
│   │   └── Brand Safety & Fraud Detection
│   └── Creative Strategy
│       ├── Creative Performance Analytics
│       ├── A/B Testing & Personalization
│       ├── Dynamic Creative Optimization
│       └── Cross-channel Creative Insights
│
└── PILLAR 3: Operations & Platform
    ├── Content Operations
    │   ├── Ingestion & Processing Pipeline
    │   ├── Content Metadata & Tagging (AI)
    │   ├── Rights Management & Compliance
    │   └── Distribution Optimization
    └── Platform & Infrastructure
        ├── Streaming QoS (Buffering, Start Time)
        ├── CDN Cost Optimization
        ├── Peak Load Management
        └── Data Pipeline Freshness
```

### 4.2 Key Metrics (Dashboard Blocks)

| KPI | Category | Target/Benchmark |
|-----|----------|-----------------|
| Subscriber LTV | Subscriptions | $142.00 |
| Monthly Churn Rate | Subscriptions | 4.2% |
| Content ROI | Content | 2.8x |
| Avg CPM | Advertising | $14.20 |
| Ad Fill Rate | Advertising | 92% |
| ROAS (Campaigns) | Advertising | 3.4x |
| Catalog Utilization | Content | 68% |
| Recommendation CTR | Content | 12.4% |
| Streaming QoS (buffering %) | Platform | 0.8% |
| Monthly Active Viewers | Audience | 8.4M |
| Avg Watch Time (min/day) | Engagement | 47 min |
| Revenue Per Viewer | Revenue | $6.80/month |
| Cross-platform Match Rate | Identity | 74% |
| Ticketing Revenue | Live Events | $2.4M/month |
| Creative Win Rate (A/B) | Creative | 34% |
| Brand Safety Score | Compliance | 98.2% |
| Video Completion Rate | Ad Performance | 76% |

### 4.3 Dashboard Pages (v2)

1. **Audience Command Center** — MAV, watch time, engagement trends, cross-platform reach, audience segments
2. **Content Performance** — Content ROI, catalog utilization, recommendation effectiveness, top titles, licensing ROI
3. **Subscription & Revenue** — Subscriber LTV, churn funnel, SVOD/AVOD/TVOD mix, paywall conversion, commerce revenue
4. **Advertising & Yield** — CPM trends, fill rate, ROAS, programmatic yield, brand safety, campaign performance
5. **Creative & Campaigns** — Creative A/B results, DCO performance, multi-touch attribution, media mix insights
6. **Platform & QoS** — Streaming quality, buffering rate, CDN costs, pipeline freshness, peak load

### 4.4 Data Model

**Table: `media_iq.gold.viewer_metrics`** (~5K rows)
| Column | Type | Description |
|--------|------|-------------|
| viewer_id | STRING | Unique viewer ID |
| segment | STRING | Premium / Standard / Free / Trial |
| subscription_type | STRING | SVOD / AVOD / TVOD / Hybrid |
| region | STRING | US / UK / EU / APAC / LATAM |
| ltv | DOUBLE | Lifetime value |
| monthly_revenue | DOUBLE | Monthly revenue contribution |
| churn_risk_score | DOUBLE | 0.0-1.0 churn probability |
| avg_watch_time_min | DOUBLE | Daily avg watch time |
| content_affinity | STRING | Drama / Action / Comedy / Documentary / Sports |
| devices_used | INT | Number of devices |
| cross_platform_id | STRING | Unified identity |
| signup_date | DATE | Account creation |
| last_active | TIMESTAMP | Last activity |

**Table: `media_iq.gold.content_performance`** (~3K rows)
| Column | Type | Description |
|--------|------|-------------|
| content_id | STRING | Content identifier |
| title | STRING | Content title |
| content_type | STRING | Original / Licensed / UGC |
| genre | STRING | Drama / Action / Comedy / Documentary / Sports |
| release_date | DATE | Release date |
| total_views | BIGINT | Total views |
| avg_completion_rate | DOUBLE | Avg watch-through % |
| content_roi | DOUBLE | Revenue / cost ratio |
| recommendation_ctr | DOUBLE | Click-through from recs |
| catalog_category | STRING | New / Library / Expiring |
| licensing_cost | DOUBLE | Content cost |
| revenue_attributed | DOUBLE | Revenue attributed to this content |
| rating | DOUBLE | User rating (1-5) |
| date | DATE | Record date |

**Table: `media_iq.gold.ad_performance`** (~3K rows)
| Column | Type | Description |
|--------|------|-------------|
| campaign_id | STRING | Campaign ID |
| advertiser | STRING | Advertiser name |
| ad_format | STRING | Pre-roll / Mid-roll / Display / Native / CTV |
| targeting_type | STRING | Contextual / Behavioral / First-party / Lookalike |
| impressions | BIGINT | Impressions served |
| cpm | DOUBLE | Cost per mille |
| ctr | DOUBLE | Click-through rate |
| vcr | DOUBLE | Video completion rate |
| roas | DOUBLE | Return on ad spend |
| fill_rate | DOUBLE | Ad fill rate |
| brand_safety_score | DOUBLE | Brand safety score |
| revenue | DOUBLE | Ad revenue |
| region | STRING | Region |
| date | DATE | Record date |

**Table: `media_iq.gold.subscription_events`** (~2K rows)
| Column | Type | Description |
|--------|------|-------------|
| event_id | STRING | Event ID |
| viewer_id | STRING | Viewer ID |
| event_type | STRING | Subscribe / Upgrade / Downgrade / Cancel / Reactivate |
| plan_from | STRING | Previous plan |
| plan_to | STRING | New plan |
| monthly_price | DOUBLE | Plan price |
| churn_reason | STRING | Price / Content / Competition / Other |
| paywall_conversion | BOOLEAN | Converted at paywall |
| date | DATE | Event date |

**Table: `media_iq.gold.platform_qos`** (~2K rows)
| Column | Type | Description |
|--------|------|-------------|
| region | STRING | CDN region |
| platform | STRING | Web / iOS / Android / Smart TV / Console |
| buffering_rate_pct | DOUBLE | Buffering percentage |
| avg_start_time_sec | DOUBLE | Stream start time |
| bitrate_avg_mbps | DOUBLE | Average bitrate |
| cdn_cost_daily | DOUBLE | CDN cost |
| concurrent_streams | INT | Peak concurrent |
| error_rate_pct | DOUBLE | Playback error rate |
| date | DATE | Record date |

---

## 5. Financial Services — Major Expansion

### 5.1 Business Objective Hierarchy

```
FINANCIAL SERVICES
├── SUB-VERTICAL A: Banking
│   ├── Grow Revenue
│   │   ├── Hyper-personalization (Next-Best-Action/Offer)
│   │   ├── Loan Origination Optimization
│   │   ├── Credit Card Acquisition & Activation
│   │   ├── Deposit Growth & Pricing
│   │   └── Digital Channel Conversion
│   ├── Manage Risk
│   │   ├── Fraud Prevention (Transaction, Account Takeover, Synthetic ID)
│   │   ├── Credit Risk Modeling (PD, LGD, EAD)
│   │   ├── AML / KYC Compliance
│   │   ├── Regulatory Reporting (Basel III/IV, CECL)
│   │   └── Cybersecurity Threat Detection
│   ├── Optimize Operations
│   │   ├── CFO / Treasury Optimization
│   │   ├── Call Center Analytics (AHT, FCR, CSAT)
│   │   ├── Back Office Automation (STP Rate)
│   │   ├── Branch Performance Analytics
│   │   └── Operational Risk Monitoring
│   └── Customer Experience
│       ├── Customer 360 / Unified Profile
│       ├── Churn Prediction & Retention
│       ├── NPS / CSAT Tracking
│       └── Journey Analytics
│
├── SUB-VERTICAL B: Capital Markets
│   ├── Generate Alpha
│   │   ├── Alpha Signal Generation (NLP, Alt Data)
│   │   ├── Portfolio Construction & Optimization
│   │   ├── Trading Analytics & Execution Quality
│   │   ├── Private Markets Analytics
│   │   └── Client Distribution & Intelligence
│   ├── Manage Risk
│   │   ├── Market Risk (VaR, CVaR, Stress Testing)
│   │   ├── Counterparty Credit Risk
│   │   ├── Regulatory Compliance (MiFID II, Dodd-Frank)
│   │   ├── Fraud & Market Manipulation Detection
│   │   └── Cybersecurity & Data Protection
│   └── Optimize Operations
│       ├── Fund Operations Automation
│       ├── Treasury & Liquidity Management
│       ├── Middle Office Efficiency
│       ├── Investor Relations & Reporting
│       └── Trade Settlement (STP Rate)
│
└── SUB-VERTICAL C: Insurance
    ├── Grow Revenue
    │   ├── Distribution Optimization (Agent/Digital)
    │   ├── Underwriting Automation & Pricing
    │   ├── Personalization & Cross-sell
    │   ├── Product Innovation Analytics
    │   └── Channel Mix Optimization
    ├── Manage Risk
    │   ├── Claims Fraud Detection
    │   ├── Actuarial Risk Modeling
    │   ├── Compliance & Regulatory (Solvency II, IFRS 17)
    │   ├── Cybersecurity Risk Assessment
    │   └── Catastrophe Modeling
    └── Optimize Operations
        ├── CFO / FP&A Analytics
        ├── Claims Processing Automation
        ├── Back Office Efficiency
        ├── Call Center Optimization
        └── Provider Network Management
```

### 5.2 Key Metrics (Dashboard Blocks)

**Banking Metrics:**
| KPI | Category | Target/Benchmark |
|-----|----------|-----------------|
| Transaction Fraud Rate | Risk | 0.23% |
| Fraud Blocked (daily) | Risk | 847 |
| False Positive Rate | Risk | 0.4% |
| Net Interest Margin | Revenue | 3.1% |
| Loan Origination Volume | Revenue | $420M/month |
| Credit Card Activation Rate | Revenue | 72% |
| Customer LTV | Revenue | $4,200 |
| AML Alert Volume | Compliance | 1,240/month |
| Call Center AHT | Operations | 6.2 min |
| FCR (First Call Resolution) | Operations | 74% |
| STP Rate (Straight Through) | Operations | 89% |
| NPS Score | CX | 38 |
| Digital Adoption Rate | CX | 67% |

**Capital Markets Metrics:**
| KPI | Category | Target/Benchmark |
|-----|----------|-----------------|
| Portfolio AUM | Revenue | $24.5B |
| VaR (95%, daily) | Risk | $47M |
| Sharpe Ratio | Performance | 1.42 |
| Alpha Generated (bps) | Performance | 180 bps |
| Trade Execution Quality | Operations | 97.3% |
| STP Settlement Rate | Operations | 94% |
| Counterparty Exposure | Risk | $890M |
| Regulatory Capital Ratio | Compliance | 14.2% |

**Insurance Metrics:**
| KPI | Category | Target/Benchmark |
|-----|----------|-----------------|
| Combined Ratio | Performance | 96.4% |
| Loss Ratio | Risk | 62.1% |
| Claims Fraud Rate | Risk | 8.2% |
| Policy Retention Rate | Revenue | 87% |
| Underwriting Automation % | Operations | 64% |
| Claims Cycle Time (days) | Operations | 12.4 |
| Premium Growth (YoY) | Revenue | 8.7% |
| Solvency Ratio | Compliance | 185% |

### 5.3 Dashboard Pages (v2)

1. **Enterprise Risk Command Center** — Cross-sub-vertical: total portfolio, fraud alerts, VaR, compliance status
2. **Banking Intelligence** — Fraud detection, credit risk heatmap, loan origination funnel, customer LTV, NPS
3. **Capital Markets Analytics** — Portfolio performance, alpha attribution, VaR/CVaR, trade execution, Sharpe
4. **Insurance Operations** — Combined ratio, claims pipeline, fraud detection, underwriting automation, loss ratio
5. **Fraud & Compliance Hub** — Unified fraud view (transaction/claims/market), AML alerts, regulatory capital
6. **Customer & Distribution** — Customer 360, churn prediction, channel performance, digital adoption, journey analytics

### 5.4 Data Model (100K rows total)

**Table: `financial_services_iq.gold.transactions`** (~30K rows)
| Column | Type | Description |
|--------|------|-------------|
| transaction_id | STRING | Unique transaction ID |
| customer_id | STRING | Customer ID |
| sub_vertical | STRING | Banking / Capital Markets / Insurance |
| business_line | STRING | Retail / Commercial / Wealth / Cards |
| channel | STRING | Mobile / Web / ATM / Branch / Wire / ACH |
| amount | DOUBLE | Transaction amount |
| currency | STRING | Currency code |
| merchant_category | STRING | MCC code category |
| is_fraud | BOOLEAN | Fraud flag |
| fraud_score | DOUBLE | ML fraud probability |
| fraud_type | STRING | Transaction / Account Takeover / Synthetic ID |
| region | STRING | Region |
| timestamp | TIMESTAMP | Transaction time |

**Table: `financial_services_iq.gold.customer_profiles`** (~15K rows)
| Column | Type | Description |
|--------|------|-------------|
| customer_id | STRING | Customer ID |
| sub_vertical | STRING | Banking / Capital Markets / Insurance |
| segment | STRING | Mass / Affluent / HNW / UHNW / SMB / Corporate |
| credit_score | INT | Credit score |
| ltv | DOUBLE | Customer lifetime value |
| tenure_years | DOUBLE | Customer tenure |
| products_held | INT | Number of products |
| nps_score | INT | Net promoter score |
| churn_risk | DOUBLE | Churn probability |
| total_relationship_value | DOUBLE | Total deposits + investments |
| digital_adoption | BOOLEAN | Uses digital channels |
| next_best_action | STRING | Recommended action |
| region | STRING | Region |

**Table: `financial_services_iq.gold.credit_portfolio`** (~15K rows)
| Column | Type | Description |
|--------|------|-------------|
| loan_id | STRING | Loan identifier |
| customer_id | STRING | Customer ID |
| loan_type | STRING | Mortgage / Auto / Personal / Credit Card / Commercial |
| origination_amount | DOUBLE | Original loan amount |
| outstanding_balance | DOUBLE | Current balance |
| interest_rate | DOUBLE | Interest rate |
| credit_score_at_origination | INT | Score at origination |
| pd_score | DOUBLE | Probability of default |
| lgd_estimate | DOUBLE | Loss given default |
| delinquency_status | STRING | Current / 30DPD / 60DPD / 90DPD / Default |
| dti_ratio | DOUBLE | Debt-to-income ratio |
| origination_date | DATE | Origination date |
| maturity_date | DATE | Maturity date |
| region | STRING | Region |

**Table: `financial_services_iq.gold.portfolio_positions`** (~10K rows)
| Column | Type | Description |
|--------|------|-------------|
| position_id | STRING | Position ID |
| portfolio_id | STRING | Portfolio ID |
| asset_class | STRING | Equities / Fixed Income / Derivatives / Alternatives |
| instrument | STRING | Instrument name |
| notional_value | DOUBLE | Notional value |
| market_value | DOUBLE | Mark-to-market value |
| pnl_daily | DOUBLE | Daily P&L |
| var_contribution | DOUBLE | VaR contribution |
| beta | DOUBLE | Beta to benchmark |
| sharpe_contribution | DOUBLE | Sharpe contribution |
| sector | STRING | Industry sector |
| region | STRING | Geographic region |
| date | DATE | Valuation date |

**Table: `financial_services_iq.gold.insurance_policies`** (~15K rows)
| Column | Type | Description |
|--------|------|-------------|
| policy_id | STRING | Policy ID |
| customer_id | STRING | Customer ID |
| product_line | STRING | Auto / Home / Life / Commercial / Health |
| premium_annual | DOUBLE | Annual premium |
| coverage_amount | DOUBLE | Coverage amount |
| underwriting_score | DOUBLE | Risk score |
| automated_underwriting | BOOLEAN | Auto-underwritten |
| claims_count_12m | INT | Claims last 12 months |
| loss_ratio | DOUBLE | Loss ratio for this policy |
| retention_probability | DOUBLE | Renewal likelihood |
| distribution_channel | STRING | Agent / Direct / Digital / Broker |
| inception_date | DATE | Policy start |
| renewal_date | DATE | Next renewal |
| region | STRING | Region |

**Table: `financial_services_iq.gold.claims`** (~10K rows)
| Column | Type | Description |
|--------|------|-------------|
| claim_id | STRING | Claim ID |
| policy_id | STRING | Policy ID |
| claim_type | STRING | Auto / Property / Liability / Health / Life |
| claim_amount | DOUBLE | Claimed amount |
| paid_amount | DOUBLE | Amount paid |
| fraud_score | DOUBLE | Fraud probability |
| is_fraud | BOOLEAN | Confirmed fraud |
| status | STRING | Open / Under Review / Approved / Denied / Closed |
| cycle_time_days | DOUBLE | Days from filing to resolution |
| adjuster_id | STRING | Assigned adjuster |
| filed_date | DATE | Filing date |
| closed_date | DATE | Closure date |
| region | STRING | Region |

**Table: `financial_services_iq.gold.compliance_events`** (~5K rows)
| Column | Type | Description |
|--------|------|-------------|
| event_id | STRING | Event ID |
| sub_vertical | STRING | Banking / Capital Markets / Insurance |
| event_type | STRING | AML Alert / KYC Review / Regulatory Filing / SAR |
| severity | STRING | Low / Medium / High / Critical |
| status | STRING | Open / Investigating / Resolved / Escalated |
| regulatory_framework | STRING | Basel III / Dodd-Frank / MiFID II / Solvency II / CECL |
| capital_impact | DOUBLE | Impact on regulatory capital |
| resolution_time_hours | DOUBLE | Time to resolve |
| auto_resolved | BOOLEAN | Resolved by automation |
| date | DATE | Event date |

---

## 6. Health & Life Sciences — Rename + Remodel

### 6.1 Business Objective Hierarchy

```
HEALTH & LIFE SCIENCES
├── SUB-VERTICAL A: Providers
│   ├── Optimize Operations
│   │   ├── Bed Utilization & Capacity Planning
│   │   ├── OR Scheduling Optimization
│   │   ├── Revenue Cycle Management (Denial Rate, A/R Days)
│   │   ├── Supply Chain & Inventory Optimization
│   │   ├── Workforce Scheduling & Productivity
│   │   └── Length of Stay Optimization
│   ├── Improve Provider Quality
│   │   ├── Clinical Decision Support
│   │   ├── Care Pathway Compliance
│   │   ├── Physician Performance Benchmarking
│   │   ├── Quality Measure Reporting (HEDIS, CMS Stars)
│   │   └── Hospital-Acquired Condition Prevention
│   └── Improve Patient Outcomes
│       ├── Readmission Risk Prediction
│       ├── Sepsis Early Warning
│       ├── Patient Deterioration Detection
│       ├── Social Determinants of Health (SDoH) Integration
│       └── Patient Experience (HCAHPS Score)
│
├── SUB-VERTICAL B: Health Plans (Payers)
│   ├── Optimize Operations
│   │   ├── Claims Processing Automation
│   │   ├── Prior Authorization Optimization
│   │   ├── Medical Cost Management
│   │   ├── Provider Network Adequacy
│   │   └── Enrollment & Billing Accuracy
│   ├── Improve Provider Quality
│   │   ├── Provider Risk Scoring
│   │   ├── Network Performance Benchmarking
│   │   ├── Value-Based Care Monitoring
│   │   ├── Fraud, Waste & Abuse Detection
│   │   └── Star Ratings Optimization
│   └── Improve Member Outcomes
│       ├── Care Gap Identification
│       ├── High-Risk Member Stratification
│       ├── Medication Adherence Programs
│       ├── Social Determinants Integration
│       └── Member Engagement & Retention
│
├── SUB-VERTICAL C: BioPharma
│   ├── Accelerate R&D
│   │   ├── Clinical Trial Optimization (site selection, enrollment)
│   │   ├── Real-World Evidence (RWE) Analytics
│   │   ├── Drug Safety / Pharmacovigilance
│   │   ├── Biomarker Discovery
│   │   └── Target Identification & Validation
│   ├── Optimize Supply Chain
│   │   ├── Demand Forecasting
│   │   ├── Manufacturing Quality Analytics
│   │   ├── Cold Chain Monitoring
│   │   ├── Inventory Optimization
│   │   └── Serialization & Track-and-Trace
│   └── Drive Commercial Effectiveness
│       ├── HCP (Healthcare Professional) Targeting
│       ├── Sales Force Optimization
│       ├── Market Access & Pricing Analytics
│       ├── Patient Journey Mapping
│       └── Competitive Intelligence
│
└── SUB-VERTICAL D: MedTech
    ├── Accelerate Product Development
    │   ├── R&D Pipeline Analytics
    │   ├── Clinical Evidence Generation
    │   ├── Regulatory Submission Optimization
    │   └── Quality Management (CAPA, NCR)
    ├── Optimize Supply Chain
    │   ├── Demand Planning & Forecasting
    │   ├── Manufacturing Yield Optimization
    │   ├── Distribution & Logistics
    │   └── Recall Risk Prediction
    └── Drive Revenue Growth
        ├── Account-based Selling Analytics
        ├── Contract & Pricing Optimization
        ├── Procedure Volume Forecasting
        └── Field Service Optimization
```

### 6.2 Key Metrics (Dashboard Blocks)

**Provider Metrics:**
| KPI | Category | Target/Benchmark |
|-----|----------|-----------------|
| Bed Utilization | Operations | 87.3% |
| ED Wait Time (avg) | Operations | 34 min |
| Readmission Rate (30d) | Outcomes | 8.2% |
| Length of Stay (avg) | Operations | 4.2 days |
| HCAHPS Score | Patient Experience | 82/100 |
| Denial Rate | Revenue Cycle | 6.8% |
| A/R Days | Revenue Cycle | 42 days |
| Sepsis Bundle Compliance | Quality | 91% |

**Health Plan Metrics:**
| KPI | Category | Target/Benchmark |
|-----|----------|-----------------|
| Medical Loss Ratio (MLR) | Financial | 84.2% |
| Claims Processing Time (days) | Operations | 4.8 days |
| Prior Auth Turnaround (hrs) | Operations | 18 hours |
| FWA Detection Rate | Risk | 4.7% |
| Star Rating (CMS) | Quality | 4.2 stars |
| Member Retention Rate | Engagement | 89% |
| Care Gap Closure Rate | Outcomes | 72% |

**BioPharma Metrics:**
| KPI | Category | Target/Benchmark |
|-----|----------|-----------------|
| Trial Enrollment Rate | R&D | 78% of target |
| Time to Market (months) | R&D | 14.2 months |
| Manufacturing Yield | Supply Chain | 94.6% |
| Rx Fulfillment Rate | Commercial | 91% |
| HCP Engagement Score | Commercial | 3.8/5.0 |
| Adverse Event Rate | Safety | 2.1% |

**MedTech Metrics:**
| KPI | Category | Target/Benchmark |
|-----|----------|-----------------|
| R&D Pipeline Value | Development | $1.2B |
| Regulatory Approval Rate | Development | 82% |
| Manufacturing Yield | Supply Chain | 96.8% |
| Field Service Response (hrs) | Operations | 4.2 hours |
| Procedure Volume Growth | Revenue | 12% YoY |
| CAPA Closure Rate | Quality | 88% |

### 6.3 Dashboard Pages (v2)

1. **HLS Command Center** — Cross-sub-vertical: patient volume, claims pipeline, R&D pipeline, revenue trends
2. **Provider Operations** — Bed utilization, ED wait, OR scheduling, LOS, workforce, revenue cycle (denial rate, A/R)
3. **Clinical Quality & Outcomes** — Readmission risk, sepsis compliance, HCAHPS, quality measures, care pathways
4. **Health Plan Analytics** — MLR, claims processing, prior auth, FWA detection, star ratings, member engagement
5. **BioPharma Intelligence** — Trial enrollment, RWE insights, drug safety, commercial effectiveness, HCP targeting
6. **MedTech & Supply Chain** — R&D pipeline, manufacturing yield, field service, procedure volume, demand forecasting

### 6.4 Data Model

**Table: `hls_iq.gold.patient_encounters`** (~5K rows)
| Column | Type | Description |
|--------|------|-------------|
| encounter_id | STRING | Encounter ID |
| patient_id | STRING | Patient ID |
| facility | STRING | Metro General / Westside / Eastview |
| department | STRING | Emergency / ICU / Cardiology / Orthopedics / Oncology |
| encounter_type | STRING | Inpatient / Outpatient / ED / Observation |
| admission_date | TIMESTAMP | Admission timestamp |
| discharge_date | TIMESTAMP | Discharge timestamp |
| los_days | DOUBLE | Length of stay |
| ed_wait_minutes | DOUBLE | ED wait time |
| readmission_risk | DOUBLE | 30-day readmission probability |
| readmitted_30d | BOOLEAN | Actually readmitted |
| hcahps_score | INT | Patient experience score |
| primary_diagnosis | STRING | ICD-10 category |
| acuity_level | STRING | Low / Medium / High / Critical |
| sdoh_risk_factors | INT | Count of social risk factors |

**Table: `hls_iq.gold.revenue_cycle`** (~3K rows)
| Column | Type | Description |
|--------|------|-------------|
| claim_id | STRING | Claim ID |
| encounter_id | STRING | Encounter ID |
| facility | STRING | Facility |
| payer | STRING | Medicare / Medicaid / Commercial / Self-pay |
| billed_amount | DOUBLE | Billed amount |
| allowed_amount | DOUBLE | Allowed amount |
| paid_amount | DOUBLE | Paid amount |
| denial_flag | BOOLEAN | Claim denied |
| denial_reason | STRING | Denial reason code |
| ar_days | INT | Days in A/R |
| clean_claim_rate | DOUBLE | First-pass rate |
| date | DATE | Claim date |

**Table: `hls_iq.gold.health_plan_claims`** (~3K rows)
| Column | Type | Description |
|--------|------|-------------|
| claim_id | STRING | Claim ID |
| member_id | STRING | Member ID |
| plan_type | STRING | HMO / PPO / Medicare Advantage / Medicaid |
| claim_type | STRING | Medical / Pharmacy / Dental / Vision |
| total_cost | DOUBLE | Total claim cost |
| processing_time_days | DOUBLE | Processing duration |
| fwa_score | DOUBLE | Fraud/waste/abuse probability |
| fwa_flag | BOOLEAN | FWA detected |
| prior_auth_required | BOOLEAN | Prior auth needed |
| prior_auth_time_hrs | DOUBLE | Prior auth turnaround |
| star_rating_impact | STRING | Positive / Neutral / Negative |
| date | DATE | Claim date |

**Table: `hls_iq.gold.clinical_quality`** (~2K rows)
| Column | Type | Description |
|--------|------|-------------|
| facility | STRING | Facility |
| department | STRING | Department |
| measure_type | STRING | HEDIS / CMS Stars / Sepsis Bundle / HAC Prevention |
| measure_name | STRING | Specific measure |
| compliance_rate | DOUBLE | Compliance percentage |
| benchmark | DOUBLE | National benchmark |
| performance_vs_benchmark | STRING | Above / At / Below |
| improvement_trend | STRING | Improving / Stable / Declining |
| reporting_period | STRING | Quarter identifier |
| date | DATE | Record date |

**Table: `hls_iq.gold.biopharma_trials`** (~1K rows)
| Column | Type | Description |
|--------|------|-------------|
| trial_id | STRING | Trial ID |
| drug_name | STRING | Drug name |
| phase | STRING | Phase I / II / III / IV |
| therapeutic_area | STRING | Oncology / Cardiology / Neurology / Immunology |
| enrollment_target | INT | Target enrollment |
| enrollment_actual | INT | Actual enrollment |
| enrollment_rate | DOUBLE | Enrollment completion % |
| site_count | INT | Number of sites |
| adverse_event_rate | DOUBLE | AE rate |
| time_to_market_months | DOUBLE | Projected time to market |
| status | STRING | Recruiting / Active / Completed / Suspended |
| start_date | DATE | Trial start |

**Table: `hls_iq.gold.medtech_products`** (~1K rows)
| Column | Type | Description |
|--------|------|-------------|
| product_id | STRING | Product ID |
| product_name | STRING | Product name |
| category | STRING | Surgical / Diagnostic / Implant / Digital Health |
| pipeline_stage | STRING | Concept / Development / Regulatory / Launched |
| pipeline_value | DOUBLE | Revenue potential |
| manufacturing_yield | DOUBLE | Production yield % |
| capa_open | INT | Open CAPAs |
| capa_closure_rate | DOUBLE | CAPA closure % |
| field_service_response_hrs | DOUBLE | Avg response time |
| procedure_volume_monthly | INT | Procedures using product |
| regulatory_status | STRING | Approved / Pending / Under Review |
| region | STRING | Region |
| date | DATE | Record date |

---

## 7. Implementation Plan

### Phase 1: Foundation (Config + Data Layer)
**Estimated scope: config files, data_access.py, data generators**

1. **Drop Manufacturing & Risk verticals**
   - Remove `config/manufacturing.yaml`, `config/risk.yaml`
   - Remove demo data generators for manufacturing and risk from `data_access.py`
   - Remove `_summarize_manufacturing` and `_summarize_risk` from `genie_backend.py`
   - Clean up `_VERTICALS` list in `main.py`

2. **Create new config files**
   - `config/telecom.yaml` — full config with app metadata, pages, genie questions, KPIs
   - `config/media.yaml` — full config with app metadata, pages, genie questions, KPIs

3. **Rename healthcare → hls**
   - Rename `config/healthcare.yaml` → `config/hls.yaml`
   - Update app title to "Blueprint Health & Life Sciences IQ"
   - Update subtitle to "Providers · Health Plans · BioPharma · MedTech"

4. **Update existing configs**
   - `config/gaming.yaml` — new pages, KPIs, genie questions aligned to outcome map
   - `config/financial_services.yaml` — expanded pages, 3 sub-vertical KPIs, new genie questions

5. **Update vertical registry**
   - `_VERTICALS` in `main.py`: `["gaming", "telecom", "media", "financial_services", "hls"]`
   - `_USE_CASE_ICONS` in `layout.py`: add telecom (`fa-tower-cell`), media (`fa-film`), update hls
   - `_USE_CASE_GENIE_DESC` in `layout.py`: add descriptions for new verticals

### Phase 2: Data Model Rebuild
**Estimated scope: data_access.py synthetic data generators**

1. **Gaming data generators** — 6 tables, ~15K rows total
   - `_generate_gaming_player_metrics()` — 5K rows
   - `_generate_gaming_revenue_metrics()` — 3K rows
   - `_generate_gaming_ua_campaigns()` — 2K rows
   - `_generate_gaming_development()` — 1K rows
   - `_generate_gaming_infrastructure()` — 2K rows
   - `_generate_gaming_live_ops()` — 2K rows

2. **Telecom data generators** — 5 tables, ~15K rows total
   - `_generate_telecom_subscribers()` — 5K rows
   - `_generate_telecom_network()` — 3K rows
   - `_generate_telecom_fraud()` — 2K rows
   - `_generate_telecom_field_ops()` — 3K rows
   - `_generate_telecom_b2b_iot()` — 2K rows

3. **Media data generators** — 5 tables, ~15K rows total
   - `_generate_media_viewers()` — 5K rows
   - `_generate_media_content()` — 3K rows
   - `_generate_media_ads()` — 3K rows
   - `_generate_media_subscriptions()` — 2K rows
   - `_generate_media_platform_qos()` — 2K rows

4. **Financial Services data generators** — 7 tables, ~100K rows total
   - `_generate_finserv_transactions()` — 30K rows
   - `_generate_finserv_customers()` — 15K rows
   - `_generate_finserv_credit_portfolio()` — 15K rows
   - `_generate_finserv_portfolio_positions()` — 10K rows
   - `_generate_finserv_insurance_policies()` — 15K rows
   - `_generate_finserv_claims()` — 10K rows
   - `_generate_finserv_compliance()` — 5K rows

5. **HLS data generators** — 6 tables, ~15K rows total
   - `_generate_hls_encounters()` — 5K rows
   - `_generate_hls_revenue_cycle()` — 3K rows
   - `_generate_hls_plan_claims()` — 3K rows
   - `_generate_hls_clinical_quality()` — 2K rows
   - `_generate_hls_biopharma_trials()` — 1K rows
   - `_generate_hls_medtech_products()` — 1K rows

### Phase 3: Dashboard Pages
**Estimated scope: page renderer functions in main.py or new page modules**

Each vertical gets 6 dashboard pages (see sections 2.3, 3.3, 4.3, 5.3, 6.3 above).

**Total: 30 dashboard pages** (5 verticals × 6 pages each)

Implementation approach:
- Extract page renderers into per-vertical modules: `app/pages/gaming.py`, `app/pages/telecom.py`, `app/pages/media.py`, `app/pages/financial_services.py`, `app/pages/hls.py`
- Each module exports a `render_<page_id>(data)` function
- Main routing callback dispatches to the correct module/function
- Shared components: KPI card builder, data table builder, chart builders (reuse existing)

### Phase 4: Genie Chat Backend
**Estimated scope: genie_backend.py**

1. **Update data summarizers**
   - Replace `_summarize_manufacturing` → remove
   - Replace `_summarize_risk` → remove
   - Update `_summarize_gaming` — include all new KPI areas
   - Update `_summarize_healthcare` → rename to `_summarize_hls` — cover 4 sub-verticals
   - Update `_summarize_financial_services` — cover Banking + Capital Markets + Insurance
   - Add `_summarize_telecom` — new summarizer
   - Add `_summarize_media` — new summarizer

2. **Update sample questions** — 10 per vertical aligned to outcome map KPIs

### Phase 5: Hub & Landing Updates
**Estimated scope: main.py hub rendering**

1. **Update `_render_hub()`** — 5 vertical cards (Gaming, Telecom, Media, Financial Services, HLS)
2. **Update vertical card stats** — show relevant KPI previews per vertical
3. **Update `_render_landing()`** — update icon previews for new lineup

### Phase 6: Testing & Deployment

1. **Update tests**
   - `test_data_access.py` — test new generators, verify row counts
   - `test_synthetic_data.py` — validate data distributions and column schemas
   - Add tests for new verticals (telecom, media)
   - Update FinServ tests for 100K row target

2. **Local testing** — verify all 30 pages render, chat works for each vertical

3. **Deploy to Databricks**
   - `databricks sync` to workspace
   - `databricks apps deploy blueprint-iq`
   - Verify in browser

---

## 8. Outcome Map → Dashboard Block Alignment Matrix

This matrix shows how every outcome map business objective maps to a visible dashboard block.

### Gaming
| Outcome Map Objective | Dashboard Page | Block/Card |
|----------------------|----------------|------------|
| Know Your Player | Player Intelligence | Player 360 profile, Segment distribution |
| Grow & Retain Playerbase | Player Intelligence | D1/D7/D30 retention curves, Churn heatmap |
| Grow Revenue | Revenue & Monetization | ARPDAU trend, IAP funnel, Battle pass engagement |
| De-risk Game Development | Game Dev & Quality | Milestone tracker, Build time chart |
| Effective Live Operations | Live Ops Command Center | Event performance, Content ROI, Economy health |
| Optimize Operations | Infrastructure & Ops | Uptime gauge, MTTR trend, Latency heatmap |
| Democratize Data | Infrastructure & Ops | Time to insight, Data quality score |

### Telecommunications
| Outcome Map Objective | Dashboard Page | Block/Card |
|----------------------|----------------|------------|
| Consumer CX | Customer Intelligence | Churn risk, NPS/CSAT, Customer 360 |
| SMB/Enterprise CX | Customer Intelligence | Account health, Upsell propensity |
| Service Delivery | Customer Intelligence | FCR rate, Order-to-Activate |
| Network Operations | Network Command Center | Uptime, Capacity, 5G rollout, Anomaly map |
| Field Operations | Field Operations | Dispatch, MTTR, Work order completion |
| Fraud Prevention | Fraud & Security | SIM swap alerts, Subscription fraud, IRSF |
| Compliance | Fraud & Security | CPNI compliance, Regulatory status |
| Monetize Beyond Connectivity | B2B & IoT | IoT devices, Edge revenue, API usage |

### Media & Entertainment
| Outcome Map Objective | Dashboard Page | Block/Card |
|----------------------|----------------|------------|
| Audience Intelligence | Audience Command Center | MAV, Segments, Cross-platform reach |
| Content Strategy | Content Performance | Content ROI, Catalog utilization, Top titles |
| Subscription Revenue | Subscription & Revenue | LTV, Churn funnel, Plan mix, Paywall |
| Ad Monetization | Advertising & Yield | CPM, Fill rate, ROAS, Programmatic yield |
| Activate & Measure | Creative & Campaigns | Attribution, Media mix, Campaign performance |
| Creative Strategy | Creative & Campaigns | A/B results, DCO, Creative insights |
| Platform Operations | Platform & QoS | Buffering rate, CDN cost, Concurrent streams |

### Financial Services
| Outcome Map Objective | Dashboard Page | Block/Card |
|----------------------|----------------|------------|
| Banking — Personalization | Banking Intelligence | Next-best-action, Customer LTV |
| Banking — Fraud | Fraud & Compliance Hub | Transaction fraud rate, Blocked count |
| Banking — Credit Risk | Banking Intelligence | PD heatmap, Delinquency waterfall |
| Capital Markets — Alpha | Capital Markets Analytics | Alpha attribution, Sharpe ratio |
| Capital Markets — Risk | Capital Markets Analytics | VaR/CVaR, Stress test results |
| Insurance — Underwriting | Insurance Operations | Combined ratio, Automation rate |
| Insurance — Claims | Insurance Operations | Claims pipeline, Cycle time, Fraud |
| Cross — Compliance | Fraud & Compliance Hub | AML alerts, Regulatory capital |
| Cross — Customer | Customer & Distribution | Customer 360, Churn, Digital adoption |

### Health & Life Sciences
| Outcome Map Objective | Dashboard Page | Block/Card |
|----------------------|----------------|------------|
| Provider — Operations | Provider Operations | Bed utilization, ED wait, OR schedule, LOS |
| Provider — Quality | Clinical Quality & Outcomes | Sepsis compliance, HEDIS, HAC prevention |
| Provider — Outcomes | Clinical Quality & Outcomes | Readmission risk, HCAHPS score |
| Health Plan — Operations | Health Plan Analytics | Claims processing, Prior auth, MLR |
| Health Plan — Quality | Health Plan Analytics | Star ratings, FWA detection |
| Health Plan — Outcomes | Health Plan Analytics | Care gaps, Member engagement |
| BioPharma — R&D | BioPharma Intelligence | Trial enrollment, RWE, Safety signals |
| BioPharma — Commercial | BioPharma Intelligence | HCP targeting, Sales force, Market access |
| MedTech — Product Dev | MedTech & Supply Chain | Pipeline value, Regulatory status |
| MedTech — Revenue | MedTech & Supply Chain | Procedure volume, Field service |

---

## 9. File Change Summary

| File | Action | Description |
|------|--------|-------------|
| `config/manufacturing.yaml` | DELETE | Vertical removed |
| `config/risk.yaml` | DELETE | Vertical removed |
| `config/healthcare.yaml` | RENAME → `config/hls.yaml` | Rename + full rewrite |
| `config/gaming.yaml` | REWRITE | Align to CMEG outcome map |
| `config/financial_services.yaml` | REWRITE | Expand to 3 sub-verticals |
| `config/telecom.yaml` | CREATE | New vertical |
| `config/media.yaml` | CREATE | New vertical |
| `app/main.py` | MAJOR EDIT | Update verticals, routing, hub, page dispatch |
| `app/data_access.py` | MAJOR EDIT | New data generators, remove old ones |
| `app/genie_backend.py` | MAJOR EDIT | New summarizers, remove old ones |
| `app/layout.py` | EDIT | Update icons, descriptions, vertical references |
| `app/pages/gaming.py` | CREATE | 6 page renderers |
| `app/pages/telecom.py` | CREATE | 6 page renderers |
| `app/pages/media.py` | CREATE | 6 page renderers |
| `app/pages/financial_services.py` | CREATE | 6 page renderers |
| `app/pages/hls.py` | CREATE | 6 page renderers |
| `app/pages/__init__.py` | CREATE | Package init |
| `tests/test_data_access.py` | EDIT | Update for new verticals |
| `tests/test_synthetic_data.py` | EDIT | Update for new data models |

---

## 10. Genie Sample Questions (v2)

### Gaming (10)
1. What is the average Player LTV by game title and segment?
2. Show me D1, D7, and D30 retention rates by acquisition source.
3. Which UA campaigns have the best ROAS in the last 30 days?
4. What is the current ARPDAU trend across all titles?
5. Which game has the highest content ROI from live events?
6. Show me bug resolution rate and build times by milestone.
7. What are the server uptime percentages by region?
8. Which player segment has the highest churn risk right now?
9. What is the CPI and CPM by acquisition channel?
10. How does battle pass engagement compare across game titles?

### Telecommunications (10)
1. What is the current customer churn rate by segment?
2. Show me network uptime by region and technology type.
3. Which regions have the highest MTTR for field operations?
4. How many SIM swap fraud attempts were detected this month?
5. What is the first-call resolution rate by service type?
6. Show me ARPU trends by plan type over the last 6 months.
7. Which cell sites have capacity utilization above 85%?
8. What is the order-to-activate cycle time by region?
9. How many IoT devices are connected by customer segment?
10. What is the NPS score distribution across consumer segments?

### Media & Entertainment (10)
1. What is the average subscriber LTV by plan type?
2. Show me content ROI for original vs licensed content.
3. Which content genres have the highest completion rate?
4. What is the current ad fill rate and CPM by ad format?
5. How does monthly churn rate vary by subscription tier?
6. What is the recommendation engine click-through rate by genre?
7. Show me streaming quality metrics (buffering rate) by platform.
8. Which advertising campaigns have the best ROAS this quarter?
9. What is the cross-platform identity match rate by region?
10. How does average watch time per day trend over the last 90 days?

### Financial Services (10)
1. What is the transaction fraud detection rate by channel?
2. Show me the credit portfolio delinquency breakdown by loan type.
3. What is the portfolio VaR contribution by asset class?
4. Which insurance product lines have the highest combined ratio?
5. How many AML alerts are open vs resolved this month?
6. What is the customer LTV distribution by segment?
7. Show me claims fraud detection rate by claim type.
8. What is the Sharpe ratio by portfolio over the last quarter?
9. Which distribution channels have the highest policy retention?
10. What is the STP settlement rate trend in capital markets?

### Health & Life Sciences (10)
1. What is the current bed utilization rate by facility?
2. Show me 30-day readmission rates by department and diagnosis.
3. What is the claims processing time by plan type?
4. Which clinical quality measures are below national benchmark?
5. How many clinical trials are currently enrolling vs target?
6. What is the denial rate by payer and denial reason?
7. Show me the fraud, waste, and abuse detection rate by claim type.
8. What is the manufacturing yield for MedTech products by category?
9. How does the ED wait time vary by facility and time of day?
10. What is the care gap closure rate by health plan?

---

## 11. Data Volume Summary

| Vertical | Tables | Total Rows | Key Expansion |
|----------|--------|------------|---------------|
| Gaming | 6 | ~15K | UA campaigns, dev metrics, live ops events |
| Telecommunications | 5 | ~15K | All new |
| Media & Entertainment | 5 | ~15K | All new |
| Financial Services | 7 | ~100K | 3 sub-verticals, deepest data model |
| Health & Life Sciences | 6 | ~15K | 4 sub-verticals, BioPharma + MedTech added |
| **TOTAL** | **29** | **~160K** | |
