# Blueprint IQ v3 Remodel Plan

## Aligning to Databricks FY27 PKO Industry Outcome Maps

> This plan maps the current app structure against official Databricks FY27 PKO outcome maps
> for each vertical and details every change needed to bring the app into alignment.

---

## Table of Contents

1. [Gaming (CMEG)](#1-gaming-cmeg)
2. [Telecom (CMEG)](#2-telecom-cmeg)
3. [Media (CMEG)](#3-media-cmeg)
4. [Financial Services](#4-financial-services)
5. [Health & Life Sciences (HLS)](#5-health--life-sciences-hls)
6. [Cross-Cutting Changes](#6-cross-cutting-changes)

---

## 1. Gaming (CMEG)

### PKO Outcome Map — 3 Pillars

| Pillar | Sub-Pillar | Use Cases |
|--------|-----------|-----------|
| **Next Gen Player Experience** | Know the Player | Player360, Player Metrics, Segmentation, Churn Mitigation, Personalization, Identity Resolution |
| | Grow the Playerbase | Player Journey Analytics, Cohort Analysis, Ads Attribution, Campaign Optimization, Multi-Touch Attribution, Ads Creative Generation, Ads Segmentation |
| | Grow Your Revenue | Next Best Offer, Purchase Optimization, Fraud Detection |
| **Build Great Games** | De-risk Development | Game Balance, Game Build Analytics, Feedback Analytics, Localization, Content Treadmill, Living NPCs |
| | Effective Live Operations | Game Health & Metrics, AB Testing, Toxicity, Anti-Cheat, Community Support, Community Safety, Game Server Operations, Content Impact Analysis |
| **Efficient Operations** | Optimize Operations | Empower Team, Strategic Initiatives, Financial Reporting, Cross Publisher Data Sharing |
| | Democratize Your Data | Self Service Analytics, AI Powered Analytics, Data Discovery, Data Sharing, Data Governance |

### PKO KPIs

Player LTV · Player Retention · ROAS · CPM · CPI · CLV · ARPDAU · Milestone Completion Rate · Bug Resolution Rate · Build Time · User Review Scores · Server Uptime · MTTR · Content ROI · Customer Acquisition Cost · Time to Insight · Data Security Risk

### Customer References

SciPlay · Mojang · Supercell · Zynga · Riot · Sega · Krafton · Devsisters · Second Dinner · Funplus

### Current vs. Proposed Page Structure

| # | Current Page | Proposed Page | Alignment |
|---|-------------|---------------|-----------|
| 1 | Live Ops Command Center (dashboard) | **Next Gen Player Experience Hub** | Reframe as pillar-level command center showing Player LTV, Retention, ARPDAU, CLV, ROAS as headline KPIs |
| 2 | Player Intelligence (player_intel) | **Know the Player** | Rename. Focus on Player360, Segmentation, Churn Mitigation, Personalization, Identity Resolution |
| 3 | User Acquisition & Growth (ua_growth) | **Grow the Playerbase** | Rename. Center on Cohort Analysis, Ads Attribution, Campaign Optimization, Multi-Touch Attribution, Ads Creative Gen |
| 4 | Revenue & Monetization (revenue) | **Grow Your Revenue** | Rename. Focus on Next Best Offer, Purchase Optimization, Fraud Detection. KPIs: CLV, ARPDAU, LTV |
| 5 | Game Development & Quality (game_dev) | **Build Great Games** | Rename. Merge De-risk Development + Effective Live Operations. KPIs: Milestone Completion Rate, Bug Resolution Rate, Build Time, User Review Scores |
| 6 | Infrastructure & Ops (infrastructure) | **Live Operations & Safety** | Refocus from generic infra to Game Health, AB Testing, Toxicity, Anti-Cheat, Community Safety, Game Server Ops. KPIs: Server Uptime, MTTR |
| 7 | Economy & Social (economy) | **Efficient Operations & Data** | Refocus to Optimize Operations + Democratize Your Data. KPIs: Time to Insight, Content ROI, Data Security Risk, Financial Reporting |

### Detailed Changes — Gaming

#### Page 1: Next Gen Player Experience Hub (dashboard)
- **KPIs to show (8):** Player LTV, Player Retention (D1/D7/D30), ARPDAU, CLV, ROAS, CPI, Active Players (DAU/MAU), High Churn Risk %
- **Charts:** Player LTV distribution by segment, Retention curves (D1-D30) by cohort
- **Tables:** Top 10 player segments by LTV, Churn risk leaderboard
- **New elements:** Outcome map navigation graphic showing the 3 pillars as clickable cards

#### Page 2: Know the Player
- **KPIs (8):** Player360 Coverage %, Segments Active, Churn Prediction Accuracy, Personalization Uplift %, Identity Resolution Match Rate, Avg Session Duration, Player Satisfaction Score, Engagement Index
- **Charts:** Player segmentation sunburst, Churn probability distribution
- **Tables:** Player360 attribute coverage, Segment performance comparison
- **Use case callouts:** Player360, Player Metrics, Segmentation, Churn Mitigation, Personalization, Identity Resolution

#### Page 3: Grow the Playerbase
- **KPIs (8):** ROAS, CPI, CPM, New Player Acquisition Rate, Campaign Conversion %, Multi-Touch Attribution Accuracy, Ads Creative CTR, Cohort D7 Retention
- **Charts:** Attribution funnel (impressions → installs → D7 retained → paying), Campaign ROI comparison
- **Tables:** Campaign performance breakdown, Cohort analysis matrix
- **Use case callouts:** Cohort Analysis, Ads Attribution, Campaign Optimization, Multi-Touch Attribution, Ads Creative Generation, Ads Segmentation

#### Page 4: Grow Your Revenue
- **KPIs (8):** ARPDAU, CLV, Purchase Conversion Rate, Next Best Offer Acceptance %, Fraud Detection Rate, Avg Transaction Value, Revenue Per Paying User, Offer Personalization Uplift
- **Charts:** Revenue waterfall by source (IAP/Ads/Subscription), NBO acceptance trend
- **Tables:** Top revenue-driving offers, Fraud detection event log
- **Use case callouts:** Next Best Offer, Purchase Optimization, Fraud Detection

#### Page 5: Build Great Games
- **KPIs (8):** Milestone Completion Rate, Bug Resolution Rate, Build Time (avg), User Review Scores, Localization Coverage %, Content Treadmill Velocity, Game Balance Score, Feedback Sentiment
- **Charts:** Build time trend, Bug resolution funnel (open → triaged → fixed → deployed)
- **Tables:** Active development milestones, Feedback analytics top themes
- **Use case callouts:** Game Balance, Game Build Analytics, Feedback Analytics, Localization, Content Treadmill, Living NPCs

#### Page 6: Live Operations & Safety
- **KPIs (8):** Server Uptime %, MTTR, AB Test Win Rate, Toxicity Detection Rate, Anti-Cheat Bans (24h), Community Reports Resolved, Game Health Index, Content Impact Score
- **Charts:** Server performance time series, AB test lift comparison
- **Tables:** Active AB tests and results, Toxicity incident log
- **Use case callouts:** Game Health & Metrics, AB Testing, Toxicity, Anti-Cheat, Community Support, Community Safety, Game Server Operations

#### Page 7: Efficient Operations & Data
- **KPIs (8):** Time to Insight, Data Security Risk Score, Self-Service Query Volume, Cross-Publisher Data Shares, Content ROI, Financial Reporting Accuracy, Data Governance Score, AI Analytics Adoption %
- **Charts:** Time to insight trend, Data governance compliance over time
- **Tables:** Top self-service queries, Cross-publisher sharing activity
- **Use case callouts:** Self Service Analytics, AI Powered Analytics, Data Discovery, Data Sharing, Data Governance, Financial Reporting

---

## 2. Telecom (CMEG)

### PKO Outcome Map — 3 Pillars

| Pillar | Sub-Pillar | Use Cases |
|--------|-----------|-----------|
| **Enhance CX to Drive Revenue** | Consumer CX & Growth | Churn Prediction & Retention, Hyper Personalized NBO (B2C), Data Monetization, Call Center Transcript Analysis, Store Performance |
| | B2B / SMB / Enterprise | Pricing & Quoting, Processing of MACs, AI Driven Order Tracking, Hyper Personalized NBO (B2B), Wholesale to Retail Fiber |
| **Efficiency: Network & Field Ops** | Network & Field Operations | Network Threat Detection, Self Healing Networks, Genie for Networks, Delta Share (Nokia & Telco), Energy Efficiency Optimization |
| **Protecting the Telecom** | Fraud Prevention | Fraud Detection |
| | Cyber Security | Cyber Monitoring |

### PKO KPIs

Subscriber Churn Rate · NBO Conversion Rate · ARPU · Data Monetization Revenue · Call Center Resolution Rate · Store Revenue Per Sq Ft · Quote Accuracy · MAC Processing Time · Order Tracking Accuracy · Network Uptime · MTTR · Energy Cost Per GB · Fraud Detection Rate · Cyber Threat Response Time

### Current vs. Proposed Page Structure

| # | Current Page | Proposed Page | Alignment |
|---|-------------|---------------|-----------|
| 1 | Network Command Center (dashboard) | **Telecom Outcome Hub** | Reframe. Headline KPIs: Subscriber Churn, ARPU, Network Uptime, Fraud Blocked. Show 3-pillar outcome map |
| 2 | Customer Intelligence (customer) | **Consumer CX & Growth** | Rename. Focus on Churn Prediction, Hyper Personalized NBO (B2C), Data Monetization, Call Center Transcript Analysis, Store Performance |
| 3 | Revenue & Growth (revenue) | **B2B / SMB / Enterprise** | Refocus from generic revenue to B2B use cases: Pricing/Quoting, MACs Processing, AI Order Tracking, NBO B2B, Wholesale to Retail Fiber |
| 4 | B2B & IoT (b2b_iot) | **Network & Field Operations** | Refocus. Center on Network Threat Detection, Self Healing Networks, Genie for Networks, Delta Share, Energy Efficiency |
| 5 | Field Operations (field_ops) | **Network Intelligence** | Refocus as deep-dive into network analytics: Self Healing Networks metrics, Energy Efficiency, Delta Share integration, Network Genie |
| 6 | Fraud & Security (fraud) | **Protecting the Telecom** | Rename. Combine Fraud Prevention + Cyber Security. KPIs: Fraud Detection Rate, Cyber Threat Response Time |
| 7 | Digital Experience (digital_experience) | **Data Monetization & Insights** | Refocus. Center on Data Monetization revenue streams, subscriber data analytics, partner insights, self-service analytics |

### Detailed Changes — Telecom

#### Page 1: Telecom Outcome Hub (dashboard)
- **KPIs (8):** Subscriber Base, Monthly Churn Rate, ARPU, Network Uptime %, Fraud Blocked (24h), NBO Conversion Rate, Energy Cost Per GB, Cyber Threats Detected
- **Charts:** Churn trend with prediction overlay, ARPU by segment (Consumer/B2B/Enterprise)
- **Tables:** Pillar-level outcome scorecard, Top churn risk accounts
- **New:** 3-pillar outcome map visual (CX → Efficiency → Protect)

#### Page 2: Consumer CX & Growth
- **KPIs (8):** Churn Prediction Accuracy, NBO Acceptance Rate (B2C), Data Monetization Revenue, Call Center First-Call Resolution, Store Performance Index, Customer Satisfaction (CSAT), Personalization Uplift %, Subscriber Growth Rate
- **Charts:** Churn probability distribution, NBO conversion funnel
- **Tables:** Top churn-risk subscribers, Store performance ranking
- **Use case callouts:** Churn Prediction & Retention, Hyper Personalized NBO B2C, Data Monetization, Call Center Transcript Analysis, Store Performance

#### Page 3: B2B / SMB / Enterprise
- **KPIs (8):** Quote Accuracy %, MAC Processing Time (avg), AI Order Tracking Accuracy, NBO Acceptance (B2B), Wholesale Fiber Provisioning Time, B2B Revenue Growth, Enterprise Contract Renewal Rate, SMB Acquisition Rate
- **Charts:** B2B revenue by segment, MAC processing time trend
- **Tables:** Enterprise deal pipeline, Wholesale fiber activation log
- **Use case callouts:** Pricing & Quoting, Processing of MACs, AI Driven Order Tracking, Hyper Personalized NBO B2B, Wholesale to Retail Fiber

#### Page 4: Network & Field Operations
- **KPIs (8):** Network Uptime %, MTTR, Self-Healing Resolution Rate, Network Threat Detection Rate, Energy Efficiency (cost/GB), Delta Share Partners, Field Dispatch Accuracy, Predictive Maintenance Alerts
- **Charts:** Network uptime heatmap, Energy efficiency trend
- **Tables:** Self-healing event log, Delta Share partner activity
- **Use case callouts:** Network Threat Detection, Self Healing Networks, Genie for Networks, Delta Share, Energy Efficiency Optimization

#### Page 5: Network Intelligence
- **KPIs (8):** Cell Tower Utilization, Spectrum Efficiency, 5G Coverage %, Capacity Planning Accuracy, Traffic Anomaly Detection Rate, RAN Optimization Score, Edge Compute Utilization, Network Latency (p99)
- **Charts:** Network topology health map, Traffic pattern analysis
- **Tables:** Cell site performance ranking, Capacity forecast vs actual
- **Deep dive:** Self Healing Networks analytics, Genie for Networks query history

#### Page 6: Protecting the Telecom
- **KPIs (8):** Fraud Detection Rate, Fraud Losses Prevented ($), SIM Swap Fraud Blocked, Cyber Threat Response Time, Security Incidents (24h), Compliance Score, Phishing Attempts Blocked, Insider Threat Alerts
- **Charts:** Fraud detection trend, Cyber threat severity distribution
- **Tables:** Recent fraud cases, Cyber incident timeline
- **Use case callouts:** Fraud Detection, Cyber Monitoring

#### Page 7: Data Monetization & Insights
- **KPIs (8):** Data Monetization Revenue, Partner Data Shares, Self-Service Query Volume, Subscriber Insight Products, Data Quality Score, Analytics Adoption Rate, Revenue Per Data Product, New Data Products Launched
- **Charts:** Data monetization revenue trend, Partner engagement funnel
- **Tables:** Active data products, Partner usage analytics
- **Use case callouts:** Data Monetization, Self Service Analytics, AI Powered Analytics

---

## 3. Media (CMEG)

### PKO Context

The CMEG deck covers Media & Entertainment as part of the broader CMEG vertical. While less detailed than Gaming and Telecom in the deck, the Media vertical should align with these key themes:

### Outcome Map — Key Themes

| Theme | Focus Areas |
|-------|------------|
| **Audience Intelligence** | Audience segmentation, Viewer behavior analytics, Cross-platform identity, Engagement scoring |
| **Content Strategy** | Content performance analytics, Content recommendation, Catalog optimization, Rights management |
| **Revenue Optimization** | Subscription analytics, Ad yield optimization, Dynamic pricing, Churn prevention |
| **Personalization & AI** | Recommendation engines, Personalized content discovery, AI-driven creative, Next best content |
| **Platform Operations** | QoS monitoring, CDN optimization, Stream quality, Capacity planning |

### Current vs. Proposed Page Structure

| # | Current Page | Proposed Page | Alignment |
|---|-------------|---------------|-----------|
| 1 | Audience Command Center (dashboard) | **Media Outcome Hub** | Reframe with outcome map visual. KPIs: MAU, Subscriber LTV, Avg CPM, Churn Rate |
| 2 | Content Performance (content) | **Content Strategy & Performance** | Expand. Add Content ROI, Rights utilization, Catalog depth analytics, Content treadmill velocity |
| 3 | Subscription & Revenue (subscriptions) | **Subscription Intelligence** | Refocus. Add LTV segmentation, Dynamic pricing analytics, Plan migration funnels, Win-back effectiveness |
| 4 | Advertising & Yield (advertising) | **Ad Yield & Data Monetization** | Expand. Add programmatic yield optimization, First-party data activation, Data clean room analytics |
| 5 | Creative & Campaigns (creative) | **Audience Intelligence** | Major refocus. Center on audience segmentation, cross-platform identity, engagement scoring, viewer journey |
| 6 | Platform & QoS (platform) | **Platform & Delivery** | Keep mostly same. Add CDN optimization, Stream quality scoring, Capacity planning, Edge compute |
| 7 | Personalization & AI (personalization) | **Personalization & AI** | Keep. Enhance with recommendation engine metrics, Next Best Content, AI-driven creative, A/B test results |

### Detailed Changes — Media

#### Page 1: Media Outcome Hub (dashboard)
- **KPIs (8):** Monthly Active Viewers, Subscriber LTV, Avg CPM, Monthly Churn Rate, Content Engagement Score, Ad Fill Rate, Recommendation CTR, Platform Uptime %
- **Charts:** Viewer growth trend with churn overlay, Revenue mix waterfall (subscription/ads/licensing)
- **Tables:** Outcome scorecard by theme, Top trending content

#### Page 2: Content Strategy & Performance
- **KPIs (8):** Content ROI, Catalog Utilization %, Rights Cost Per View, Content Freshness Score, Avg View Duration, Completion Rate, Content Treadmill Velocity, Originals vs Licensed Performance
- **Charts:** Content performance matrix (cost vs engagement), Content lifecycle curve
- **Tables:** Top performing content by ROI, Rights expiration calendar

#### Page 3: Subscription Intelligence
- **KPIs (8):** Subscriber LTV, Plan Conversion Rate, Win-Back Success %, Free-to-Paid Conversion, ARPU by Plan Tier, Trial-to-Paid %, Voluntary Churn Rate, Involuntary Churn Rate
- **Charts:** LTV distribution by segment, Plan migration sankey diagram
- **Tables:** Cohort retention matrix, Dynamic pricing test results

#### Page 4: Ad Yield & Data Monetization
- **KPIs (8):** Avg CPM, Ad Fill Rate, Programmatic Yield %, First-Party Data Segments, Data Clean Room Partners, eCPM Trend, Viewability Rate, Ad Revenue Per Viewer
- **Charts:** CPM trend by format, Programmatic vs direct deal mix
- **Tables:** Top advertiser performance, Data clean room partner activity

#### Page 5: Audience Intelligence
- **KPIs (8):** Unique Viewers, Cross-Platform Identity Match Rate, Engagement Score (avg), Audience Segments Active, Viewer Journey Completion %, Demographic Reach Index, Behavioral Cohorts, Attention Score
- **Charts:** Audience segmentation sunburst, Cross-platform viewer journey
- **Tables:** Segment performance comparison, Viewer behavior clusters

#### Page 6: Platform & Delivery
- **KPIs (8):** Stream Start Time (p50), Rebuffer Rate, CDN Hit Ratio, Platform Uptime %, Edge Compute Utilization, Concurrent Stream Peak, QoE Score, Capacity Headroom %
- **Charts:** QoE heatmap by region, CDN performance trend
- **Tables:** Regional delivery performance, Capacity forecast vs actual

#### Page 7: Personalization & AI
- **KPIs (8):** Recommendation CTR, Next Best Content Accuracy, AI Creative Generation Volume, A/B Test Win Rate, Personalization Uplift %, Model Refresh Frequency, Cold Start Resolution Rate, User Preference Coverage
- **Charts:** Recommendation accuracy over time, A/B test lift distribution
- **Tables:** Active recommendation models, Personalization experiment results

---

## 4. Financial Services

### PKO Outcome Map — Capital Markets Focus

| Pillar | Sub-Pillar | Use Cases |
|--------|-----------|-----------|
| **Drive Growth** | Investment Analytics & Alpha Generation | Quant Research, Alternative Data Analytics, Alpha Signal Discovery, Portfolio Construction |
| | Investment Advisory | Client 360, Personalized Recommendations, Risk-Adjusted Advisory, Wealth Management Analytics |
| | Trading Analytics | Trade Execution Analytics, Market Microstructure, Algorithmic Trading Optimization, Pre/Post-Trade Analysis |
| **Protect the Firm** | Risk Management | Market Risk, Credit Risk, Counterparty Risk, Liquidity Risk, Stress Testing |
| | Regulatory Compliance | Regulatory Reporting, Trade Surveillance, KYC/AML, Compliance Monitoring |
| | Fraud Prevention | Transaction Fraud Detection, Account Takeover, Synthetic Identity Fraud |
| | Cybersecurity | Threat Detection, Insider Trading Surveillance, Data Loss Prevention |
| **Drive Efficiency** | CFO & Treasury | Cash Flow Forecasting, Balance Sheet Optimization, FP&A Analytics |
| | Back/Middle Office Automation | Trade Settlement, Reconciliation, Client Onboarding, Document Processing |

### PKO KPIs

Alpha Generation · Sharpe Ratio · Portfolio Turnover · Client AUM Growth · Trading Cost (bps) · VaR Accuracy · Stress Test Coverage · Regulatory Reporting Timeliness · Fraud Detection Rate · False Positive Rate · STP Rate · Reconciliation Break Rate · Time to Onboard

### Current vs. Proposed Page Structure

| # | Current Page | Proposed Page | Alignment |
|---|-------------|---------------|-----------|
| 1 | Enterprise Risk Command Center (dashboard) | **Capital Markets Outcome Hub** | Reframe. Show 3-pillar outcome map (Growth/Protect/Efficiency). Headline KPIs from each pillar |
| 2 | Banking Intelligence (banking) | **Investment Analytics & Alpha** | Major refocus. Center on Quant Research, Alternative Data, Alpha Signal Discovery, Portfolio Construction |
| 3 | Capital Markets Analytics (capital_markets) | **Trading & Advisory** | Refocus. Combine Trading Analytics + Investment Advisory. KPIs: Trading Cost, Client AUM, Sharpe Ratio |
| 4 | Insurance Operations (insurance) | **Risk Management** | Refocus from insurance to cross-cutting risk: Market Risk, Credit Risk, Counterparty Risk, Liquidity Risk, Stress Testing |
| 5 | Fraud & Compliance Hub (fraud_compliance) | **Regulatory & Compliance** | Rename. Focus on Regulatory Reporting, Trade Surveillance, KYC/AML, Compliance Monitoring |
| 6 | Customer & Distribution (customer) | **Fraud & Cybersecurity** | Refocus. Combine Fraud Prevention + Cybersecurity: Transaction Fraud, Account Takeover, Synthetic Identity, Threat Detection |
| 7 | Payments & Cards (payments) | **Operations & Efficiency** | Refocus. Center on CFO & Treasury + Back/Middle Office: STP Rate, Reconciliation, Client Onboarding, Document Processing |

### Detailed Changes — Financial Services

#### Page 1: Capital Markets Outcome Hub (dashboard)
- **KPIs (8):** Alpha Generation (bps), Portfolio VaR (95%), Fraud Detection Rate, STP Rate, Trading Cost (bps), Regulatory Report Timeliness, AUM Growth %, Reconciliation Break Rate
- **Charts:** P&L attribution waterfall, Risk exposure heatmap by asset class
- **Tables:** 3-pillar outcome scorecard, Top risk alerts
- **New:** Outcome map visual showing Drive Growth → Protect → Efficiency

#### Page 2: Investment Analytics & Alpha
- **KPIs (8):** Alpha vs Benchmark (bps), Sharpe Ratio, Information Ratio, Alternative Data Signals Active, Signal Hit Rate, Portfolio Turnover, Factor Exposure Score, Research Pipeline Velocity
- **Charts:** Alpha generation over time, Factor contribution decomposition
- **Tables:** Active alpha signals and performance, Alternative data source inventory
- **Use case callouts:** Quant Research, Alternative Data Analytics, Alpha Signal Discovery, Portfolio Construction

#### Page 3: Trading & Advisory
- **KPIs (8):** Trading Cost (bps), Implementation Shortfall, Client AUM Growth, Advisory Revenue, Fill Rate, Algorithmic Trade %, Client Satisfaction Score, Personalization Adoption
- **Charts:** Execution quality trend, Client AUM distribution by segment
- **Tables:** Trade execution analysis, Client advisory recommendations
- **Use case callouts:** Trade Execution Analytics, Market Microstructure, Algorithmic Trading, Client 360, Personalized Recommendations

#### Page 4: Risk Management
- **KPIs (8):** VaR (95%), VaR Backtest Pass Rate, Credit Exposure, Counterparty Risk Score, Liquidity Coverage Ratio, Stress Test Scenarios Passed, Expected Shortfall, Risk-Adjusted Return
- **Charts:** VaR backtesting chart, Stress test scenario comparison
- **Tables:** Top risk positions, Counterparty exposure matrix
- **Use case callouts:** Market Risk, Credit Risk, Counterparty Risk, Liquidity Risk, Stress Testing

#### Page 5: Regulatory & Compliance
- **KPIs (8):** Regulatory Report Timeliness (%), Trade Surveillance Alerts, KYC Completion Rate, AML Suspicious Activity Reports, Compliance Score, Audit Findings Open, Regulatory Exam Readiness, Data Lineage Coverage
- **Charts:** Compliance score trend, Surveillance alert volume by type
- **Tables:** Regulatory reporting calendar, Open compliance issues
- **Use case callouts:** Regulatory Reporting, Trade Surveillance, KYC/AML, Compliance Monitoring

#### Page 6: Fraud & Cybersecurity
- **KPIs (8):** Fraud Detection Rate, False Positive Rate, Fraud Losses Prevented ($), Account Takeover Blocked, Synthetic Identity Detections, Cyber Threat Response Time, Data Loss Prevention Events, Insider Threat Alerts
- **Charts:** Fraud detection trend, Threat severity distribution
- **Tables:** Recent fraud cases, Cybersecurity incident log
- **Use case callouts:** Transaction Fraud Detection, Account Takeover, Synthetic Identity Fraud, Threat Detection, Insider Trading Surveillance

#### Page 7: Operations & Efficiency
- **KPIs (8):** STP Rate, Reconciliation Break Rate, Client Onboarding Time (days), Document Processing Automation %, Cash Flow Forecast Accuracy, Settlement Failure Rate, FP&A Cycle Time, Balance Sheet Optimization Score
- **Charts:** STP rate improvement trend, Onboarding time distribution
- **Tables:** Outstanding reconciliation breaks, Document processing queue
- **Use case callouts:** Trade Settlement, Reconciliation, Client Onboarding, Document Processing, Cash Flow Forecasting, FP&A Analytics

---

## 5. Health & Life Sciences (HLS)

### PKO Outcome Maps — 3 Sub-Verticals

#### BioPharma

| Pillar | Sub-Pillar | Use Cases |
|--------|-----------|-----------|
| **Increase R&D Productivity** | Accelerate Drug Discovery | Target Identification, Molecular Simulation, Literature Mining, Biomarker Discovery |
| | Streamline Clinical Development | Trial Design Optimization, Patient Recruitment, Site Selection, Safety Signal Detection |
| | Build FAIR Data Platform | Data Harmonization, Metadata Management, FAIR Compliance, Data Sharing |
| **Optimize Supply Chain & Manufacturing** | E2E Supply Chain Visibility | Demand Forecasting, Inventory Optimization, Cold Chain Monitoring, Supplier Risk |
| | Smart Manufacturing | Process Optimization, Quality Prediction, Batch Release Acceleration, Equipment Health |
| **Improve Commercial Effectiveness** | Evidence Generation & Insights | RWE Analytics, HEOR Studies, Publication Analytics, KOL Identification |
| | Deliver Provider Next-Best-Action | HCP Engagement, Detailing Optimization, Channel Mix, Field Force Effectiveness |
| | Personalize Patient Engagement | Patient Journey Mapping, Adherence Prediction, Hub Services, Patient Support |

#### MedTech

| Pillar | Sub-Pillar | Use Cases |
|--------|-----------|-----------|
| **Accelerate Product Development** | Improve Device Design | Design Optimization, Simulation, Complaint Analysis, Usability Analytics |
| | Streamline Trials & Postmarket | Clinical Evidence, Postmarket Surveillance, Adverse Event Detection |
| | Innovate with Digital Surgery | Surgical Planning, Intraoperative Analytics, Outcome Prediction |
| **Optimize Supply Chain & Manufacturing** | Dual Supply Chain Visibility | Demand Planning, Distribution Optimization, Consignment Tracking, UDI Compliance |
| | Smart Manufacturing | Process Control, Yield Optimization, Predictive Maintenance |
| **Drive Revenue Growth** | Evidence Generation | Health Economics, Outcome Studies, Competitive Benchmarking |
| | Deliver Next-Best-Action | Surgeon Engagement, Account Planning, Contract Optimization |
| | Personalize Patient Engagement | Patient Outcomes Tracking, Remote Monitoring, Digital Therapeutics |

#### Healthcare (Payer/Provider)

| Pillar | Sub-Pillar | Use Cases |
|--------|-----------|-----------|
| **Streamline Operations** | Benefits & Payments | Claims Processing, Payment Integrity, Prior Authorization, Member Eligibility |
| | Clinical Note Processing | NLP for Clinical Notes, Coding Optimization, Documentation Quality |
| **Enhance Network & Quality** | Provider Quality | Quality Measure Reporting, Star Rating Optimization, Network Adequacy |
| | Population Health | Risk Stratification, Care Gap Identification, Social Determinants, Disease Prediction |
| **Improve Outcomes & Experience** | Patient Engagement | Care Coordination, Digital Front Door, Patient Communication, Satisfaction Analytics |

### Current vs. Proposed Page Structure

| # | Current Page | Proposed Page | Alignment |
|---|-------------|---------------|-----------|
| 1 | HLS Command Center (dashboard) | **HLS Outcome Hub** | Reframe. Show 3 sub-vertical tabs (BioPharma / MedTech / Healthcare) with outcome map visuals |
| 2 | Provider Operations (provider_ops) | **Healthcare Operations** | Refocus. Center on Benefits & Payments, Clinical Note Processing (NLP). KPIs: Claims STP Rate, Prior Auth Turnaround, Coding Accuracy |
| 3 | Clinical Quality & Outcomes (clinical_quality) | **Network & Quality** | Refocus. Center on Provider Quality, Star Rating, Network Adequacy, Quality Measure Reporting |
| 4 | Health Plan Analytics (health_plans) | **BioPharma Intelligence** | Major refocus. Center on R&D Productivity + Commercial Effectiveness. Drug Discovery, Clinical Trials, RWE, Provider NBA |
| 5 | BioPharma Intelligence (biopharma) | **MedTech & Digital Surgery** | Refocus. Center on Product Development, Trials & Postmarket, Digital Surgery, Smart Manufacturing |
| 6 | MedTech & Supply Chain (medtech) | **Supply Chain & Manufacturing** | Refocus. Combine BioPharma + MedTech supply chain. E2E Visibility, Smart Manufacturing, Cold Chain, Equipment Health |
| 7 | Population Health (population_health) | **Patient Outcomes & Engagement** | Expand. Combine Population Health + Patient Engagement across all 3 sub-verticals |

### Detailed Changes — HLS

#### Page 1: HLS Outcome Hub (dashboard)
- **KPIs (8):** Bed Utilization, Readmission Rate (30-day), Star Rating, Claims FWA Rate, Drug Pipeline Velocity, Manufacturing Yield, Patient Satisfaction, R&D Cost Per Molecule
- **Charts:** Outcome map by sub-vertical, Quality metrics trend
- **Tables:** Sub-vertical scorecard, Top priority alerts
- **New:** Sub-vertical selector tabs (BioPharma / MedTech / Healthcare)

#### Page 2: Healthcare Operations
- **KPIs (8):** Claims STP Rate, Prior Authorization Turnaround (hrs), Payment Integrity Savings ($), Clinical Note NLP Accuracy, Coding Optimization Uplift %, Member Eligibility Accuracy, Documentation Quality Score, Auto-Adjudication Rate
- **Charts:** Claims processing funnel, Prior auth turnaround distribution
- **Tables:** Payment integrity findings, Clinical note processing queue
- **Use case callouts:** Claims Processing, Payment Integrity, Prior Authorization, NLP for Clinical Notes, Coding Optimization

#### Page 3: Network & Quality
- **KPIs (8):** Star Rating (overall), Quality Measure Pass Rate, Network Adequacy %, Provider Satisfaction, HEDIS Measure Compliance, Risk Adjustment Accuracy, Credential Verification Time, Value-Based Contract Performance
- **Charts:** Star rating component breakdown, Quality measure trends
- **Tables:** Provider quality scorecard, Network gap analysis
- **Use case callouts:** Quality Measure Reporting, Star Rating Optimization, Network Adequacy, Provider Quality

#### Page 4: BioPharma Intelligence
- **KPIs (8):** Drug Pipeline Velocity, Clinical Trial Enrollment Rate, RWE Study Completion %, Provider NBA Acceptance Rate, Patient Adherence %, Field Force Effectiveness, Biomarker Discovery Rate, Safety Signal Detection Time
- **Charts:** Drug pipeline funnel (Discovery → Preclinical → Phase I/II/III → Approval), RWE evidence generation trend
- **Tables:** Active clinical trials, Provider engagement scorecard
- **Use case callouts:** Drug Discovery, Clinical Development, RWE Analytics, Provider NBA, Patient Engagement, FAIR Data Platform

#### Page 5: MedTech & Digital Surgery
- **KPIs (8):** Device Approval Timeline, Postmarket Surveillance Alert Rate, Digital Surgery Procedures, Complaint Resolution Time, Adverse Event Detection Rate, Surgical Outcome Prediction Accuracy, UDI Compliance %, Clinical Evidence Studies Active
- **Charts:** Device lifecycle performance, Surgical outcome prediction vs actual
- **Tables:** Postmarket surveillance events, Digital surgery case log
- **Use case callouts:** Device Design, Trials & Postmarket, Digital Surgery, Adverse Event Detection, Outcome Prediction

#### Page 6: Supply Chain & Manufacturing
- **KPIs (8):** Manufacturing Yield, Demand Forecast Accuracy, Cold Chain Compliance %, Equipment OEE, Batch Release Time (days), Inventory Turns, Supplier Risk Score, Consignment Utilization
- **Charts:** Supply chain visibility map (raw materials → manufacturing → distribution → patient), Yield trend by product line
- **Tables:** Inventory health by SKU, Predictive maintenance schedule
- **Use case callouts:** E2E Supply Chain Visibility, Smart Manufacturing, Cold Chain Monitoring, Equipment Health, Demand Forecasting

#### Page 7: Patient Outcomes & Engagement
- **KPIs (8):** Patient Satisfaction (NPS), Risk Stratification Accuracy, Care Gap Closure Rate, Social Determinants Coverage, Disease Prediction Accuracy, Patient Adherence Rate, Digital Front Door Adoption, Care Coordination Score
- **Charts:** Risk stratification pyramid, Care gap closure trend
- **Tables:** Population health cohort analysis, Patient engagement activity log
- **Use case callouts:** Risk Stratification, Care Gap Identification, Social Determinants, Patient Journey Mapping, Adherence Prediction, Digital Front Door

---

## 6. Cross-Cutting Changes

### Config YAML Updates (all verticals)

For each vertical's `config/{vertical}.yaml`:

1. **Pages section:** Update `id`, `label`, and `icon` to match the new page names above
2. **Dashboard KPIs:** Update to use the 8 KPIs defined for each vertical's hub page (currently only 4)
3. **Genie sample questions:** Update 100 questions to reflect the new PKO-aligned use cases and terminology
4. **Genie tables:** Update table references to match any renamed tables in the data layer
5. **ML models:** Add/update model references to match PKO use cases (e.g., Alpha Signal Discovery for FinServ, Drug Discovery for HLS)

### Page Module Updates (all verticals)

For each `app/pages/{vertical}.py`:

1. **Rename render functions** to match new page IDs
2. **Update KPIs** in each render function to match the 8 KPIs defined above per page
3. **Update charts** to match the 2 charts defined above per page
4. **Update tables** to match the 2 tables defined above per page
5. **Add use case callout sections** — small cards or badges showing which Databricks use cases the page demonstrates
6. **Add customer reference logos/names** where applicable (Gaming: SciPlay, Mojang, etc.)

### Data Layer Updates

1. **Synthetic data generators** may need updates to produce data matching new KPI names
2. **Demo mode data** should reflect the renamed metrics
3. **Table schemas** in config should align with new page structures

### UI/UX Enhancements

1. **Outcome map visualization** on each dashboard hub page — interactive graphic showing the pillar structure
2. **Use case badges** on each detail page — showing which PKO use cases are demonstrated
3. **Customer reference section** — logos or names of Databricks customers in each vertical
4. **Pillar navigation** — consider grouping pages by pillar in the sidebar (with section headers)

### Implementation Priority

| Priority | Scope | Description |
|----------|-------|-------------|
| P0 | All verticals | Rename pages and update sidebar navigation to match PKO outcome maps |
| P0 | All verticals | Update dashboard hub KPIs to the 8 headline KPIs per vertical |
| P1 | All verticals | Rewrite each page's KPIs, charts, and tables to match the detailed specs above |
| P1 | All verticals | Update YAML configs (pages, dashboard KPIs, genie questions) |
| P2 | All verticals | Add outcome map visualizations to hub pages |
| P2 | All verticals | Add use case callout badges to detail pages |
| P3 | All verticals | Add customer reference sections |
| P3 | All verticals | Update synthetic data generators for new metric names |

### Estimated Scope Per Vertical

- **7 pages** × 8 KPIs = 56 KPIs per vertical (280 total)
- **7 pages** × 2 charts = 14 charts per vertical (70 total)
- **7 pages** × 2 tables = 14 tables per vertical (70 total)
- **100 genie questions** per vertical (500 total, needs review/update)
- **5 YAML configs** to update
- **5 page modules** to rewrite (~1,000-1,200 lines each)

---

*Generated from Databricks FY27 PKO decks: CMEG, Financial Services, HLS (BioPharma/MedTech), HLS (Healthcare)*
