"""Remaining 7 verticals marketing data."""
VERTICALS_2 = {
    "media": {
        "slug": "media",
        "product_name": "MediaIQ",
        "icon": "&#x1F3A5;",
        "accent": "#f59e0b",
        "hero_tagline": "Audience intelligence & content analytics for modern media",
        "hero_description": (
            "MediaIQ gives media companies deep visibility into audience behavior, content performance, "
            "and ad monetization across every screen and platform."
        ),
        "the_challenge": (
            "Media companies face a fragmented audience spread across web, mobile, smart TV, and console "
            "platforms. With 8.4 million monthly active viewers generating billions of events &mdash; plays, "
            "pauses, skips, searches, ratings &mdash; the volume of behavioral data is staggering. Yet most "
            "media analytics stacks can't answer basic questions: Which content drives subscriptions vs. churn? "
            "What is the true ROI of a $10M original series investment? Why did 4.2% of subscribers cancel last "
            "month? Ad teams operate in a separate silo from content teams, meaning a show that drives massive "
            "ad yield might be deprioritized because subscription metrics don't capture its full value. "
            "Cross-platform identity resolution remains a persistent challenge &mdash; the same viewer on mobile, "
            "web, and connected TV looks like three separate users in legacy systems."
        ),
        "the_solution": (
            "MediaIQ solves the fragmentation problem by unifying audience identity across platforms with a 74% "
            "cross-platform match rate, creating a single viewer profile that captures behavior across every screen. "
            "Content performance is measured through ROI (averaging 2.8x), not just views. A Subscriber Churn "
            "Predictor (AUC 0.91) identifies at-risk subscribers 30 days before cancellation. The Content Recommender "
            "drives a 12.4% click-through rate, more than double industry average. Ad yield optimization runs in "
            "real-time at $14.20 CPM with 92% fill rate. Every metric ties back to the north star: subscriber LTV of $142."
        ),
        "metric_deep_dives": [
            {"name": "Monthly Active Viewers", "value": "8.4M", "context": "audience scale",
             "explanation": "MAV measures total unique viewers across all platforms in a 30-day window. At 8.4M, MediaIQ tracks engagement depth (47 min average watch time), content discovery patterns, and session frequency. The platform decomposes MAV by platform (Web, iOS, Android, Smart TV, Console), subscription tier, and content genre to understand where audiences are growing and where they're declining."},
            {"name": "Subscriber LTV", "value": "$142", "context": "lifetime value",
             "explanation": "LTV captures the total revenue a subscriber generates from sign-up through churn across subscription fees, upsells, and ad impressions. At $142, MediaIQ models LTV by acquisition channel, content consumption pattern, and plan tier. Premium subscribers ($19.99/mo) have 3.2x higher LTV than Standard due to lower churn and higher engagement. This data drives content investment and acquisition budget decisions."},
            {"name": "CPM", "value": "$14.20", "context": "ad revenue per 1,000 impressions",
             "explanation": "CPM is the fundamental ad monetization metric. At $14.20 with a 92% fill rate, MediaIQ optimizes yield by daypart, content genre, audience segment, and ad format. Programmatic demand is matched against direct deals in real-time. The platform tracks viewability at 76% video completion rate and 3.4x ROAS for advertisers, ensuring premium pricing is sustainable."},
            {"name": "Churn Rate", "value": "4.2%", "context": "monthly subscriber loss",
             "explanation": "Every point of churn reduction at scale represents millions in saved revenue. At 4.2% monthly churn, MediaIQ's prediction model identifies subscribers likely to cancel 30 days in advance based on engagement decay, support contacts, billing failures, and content exhaustion signals. Retention teams receive prioritized intervention lists with recommended actions."},
            {"name": "Recommendation CTR", "value": "12.4%", "context": "content discovery",
             "explanation": "The recommendation engine's 12.4% CTR is more than double the 5% industry benchmark, driving content discovery and reducing 'browsing fatigue' that leads to session abandonment. MediaIQ's Content Recommender uses collaborative filtering enhanced with content metadata and viewing context to surface relevant titles."},
            {"name": "Content ROI", "value": "2.8x", "context": "average return on content investment",
             "explanation": "Content ROI measures subscriber value generated per dollar of content spend. At 2.8x, MediaIQ attributes revenue to specific titles by tracking which content drives new subscriptions, prevents churn, and generates ad impressions. This analysis shows that original drama series deliver 4.1x ROI vs. 1.8x for licensed content, informing greenlight decisions."},
        ],
        "page_details": [
            {"name": "Media Outcome Hub", "desc": "Executive dashboard with audience metrics, revenue breakdown, content performance, and morning briefing. Portfolio view across all content types (Original, Licensed, UGC) with trend indicators."},
            {"name": "Content Strategy", "desc": "Content catalog analytics with ROI attribution, genre performance, viewing completion rates, and content lifecycle analysis. Greenlight decision support with predictive audience modeling."},
            {"name": "Audience Intelligence", "desc": "Cross-platform audience profiling, behavioral segmentation, watch time analysis, and identity resolution metrics. 74% cross-platform match rate enables true 360-degree viewer understanding."},
            {"name": "Subscription Intelligence", "desc": "Subscriber lifecycle analytics from acquisition through renewal. Churn prediction, plan migration analysis, billing health monitoring, and trial-to-paid conversion tracking."},
            {"name": "Ad Yield Optimization", "desc": "Programmatic and direct ad performance tracking with CPM optimization, fill rate management, viewability scoring, and advertiser ROAS reporting. Connects content performance to ad revenue potential."},
            {"name": "Platform Delivery", "desc": "Quality of service monitoring with buffering rates (0.8%), startup times (1.8 sec), bitrate adaptation, and CDN performance. Correlates QoS metrics with audience retention and churn."},
            {"name": "Personalization & AI", "desc": "Recommendation engine performance, A/B test results, content discovery metrics, and personalization effectiveness. Tracks the 12.4% CTR and its impact on engagement and retention."},
        ],
        "genie_section": "MediaIQ's Genie AI understands media metrics natively. Ask 'Which original series had the highest subscriber acquisition impact last quarter?' or 'What is the churn rate for subscribers who watched less than 2 hours last month?' and get instant, data-backed answers across 255+ pre-trained questions.",
        "business_impact": [
            {"metric": "30 days", "label": "advance churn prediction window"},
            {"metric": "74%", "label": "cross-platform identity match rate"},
            {"metric": "2x", "label": "recommendation CTR vs. industry average"},
            {"metric": "5", "label": "platforms unified in one audience view"},
        ],
    },
    "risk": {
        "slug": "risk",
        "product_name": "RiskIQ",
        "icon": "&#x1F6E1;",
        "accent": "#ef4444",
        "hero_tagline": "Enterprise risk management, compliance & threat intelligence",
        "hero_description": (
            "RiskIQ provides a unified command center for enterprise risk across credit, market, operational, "
            "liquidity, and cyber dimensions &mdash; turning risk management into a strategic advantage."
        ),
        "the_challenge": (
            "Enterprise risk management has become exponentially more complex. Organizations must simultaneously "
            "monitor credit portfolios, market exposures, operational incidents, cyber threats, liquidity positions, "
            "and regulatory compliance across multiple frameworks (Basel III, COSO ERM, ISO 31000, NIST CSF, SOX). "
            "Most risk teams operate in silos &mdash; credit risk uses one system, market risk another, cyber has "
            "its own tooling &mdash; creating blind spots where correlated risks compound undetected. A cyber breach "
            "that triggers operational losses and reputational damage isn't visible in any single risk system. "
            "Meanwhile, regulatory scrutiny intensifies: risk teams spend 40% of their time on compliance reporting "
            "instead of risk analysis."
        ),
        "the_solution": (
            "RiskIQ unifies all eight risk dimensions into a single platform built on Databricks Lakehouse. "
            "ML-powered scoring runs continuously: a Credit Default Model (XGBClassifier, AUC 0.94) monitors "
            "the loan book, a Market Anomaly Detector (IsolationForest, precision 0.89) flags unusual position "
            "changes, and an Operational Loss Forecaster (BayesianTimeSeries, MAPE 12.3) predicts incident "
            "frequency. Compliance workflows are automated against five regulatory frameworks. The result is a "
            "risk command center where a CRO can see total enterprise exposure in real-time, drill into any "
            "risk dimension, and receive AI-generated morning briefings on overnight risk events."
        ),
        "metric_deep_dives": [
            {"name": "Value at Risk (99%)", "value": "$142.8M", "context": "enterprise-wide",
             "explanation": "VaR at 99% confidence represents the maximum expected loss on 99 out of 100 days. At $142.8M, RiskIQ computes this across all business units (Retail Banking, Commercial Banking, Investment Banking, Wealth Management, Treasury, Insurance, Asset Management) with real-time position updates. Stress scenarios are run on-demand against the live portfolio."},
            {"name": "Risk-Adjusted Capital", "value": "14.7%", "context": "above regulatory minimum",
             "explanation": "This ratio measures available capital against risk-weighted assets. At 14.7%, well above Basel III minimums (10.5% including buffers), RiskIQ tracks capital adequacy in real-time, alerting treasury when business growth or market moves compress the buffer. Decomposition by business unit enables capital allocation optimization."},
            {"name": "Credit Default Rate", "value": "1.23%", "context": "portfolio health",
             "explanation": "The annualized default rate across the credit portfolio. At 1.23%, RiskIQ's Credit Default Model monitors every exposure with probability of default (PD), loss given default (LGD), and exposure at default (EAD) calculations. Early warning signals flag deteriorating credits 90 days before default, enabling proactive workout and restructuring."},
            {"name": "CET1 Ratio", "value": "13.2%", "context": "common equity tier 1",
             "explanation": "CET1 is the highest-quality capital ratio and the primary measure regulators use to assess bank solvency. At 13.2%, RiskIQ provides real-time CET1 monitoring with forward-looking projections under baseline, adverse, and severely adverse scenarios."},
            {"name": "Compliance Findings", "value": "47", "context": "open items",
             "explanation": "Active compliance findings across all regulatory frameworks. RiskIQ tracks each finding from identification through remediation with owner assignment, due dates, and escalation workflows. Aging analysis ensures no finding goes unaddressed and regulatory exam preparedness is always current."},
            {"name": "Cyber Incidents", "value": "12", "context": "last 30 days",
             "explanation": "Cyber risk is the fastest-growing threat dimension. RiskIQ integrates SIEM data, threat intelligence feeds, and access logs to track incidents by severity, attack vector, and business impact. The cyber dashboard connects technical indicators to financial exposure estimates."},
        ],
        "page_details": [
            {"name": "Risk Command Center", "desc": "Enterprise-wide risk dashboard with aggregate exposure, risk appetite utilization, morning briefing, and cross-dimension correlation analysis. The CRO's single view of total risk posture."},
            {"name": "Enterprise Risk", "desc": "Holistic risk register with heat maps across all eight risk types. Risk appetite framework monitoring, key risk indicator (KRI) tracking, and board-ready reporting."},
            {"name": "Credit Risk", "desc": "Portfolio monitoring with PD/LGD/EAD calculations, concentration analysis, watchlist management, and migration matrices. The Credit Default Model (AUC 0.94) scores every exposure."},
            {"name": "Market Risk", "desc": "VaR analysis by desk and instrument, Greeks monitoring, stress testing, and scenario analysis. Historical simulation and Monte Carlo engines run against live positions."},
            {"name": "Operational Risk", "desc": "Incident tracking, loss event database, RCSA management, and KRI monitoring. The Operational Loss Forecaster predicts incident frequency and severity distributions."},
            {"name": "Compliance & Regulatory", "desc": "Automated compliance monitoring across Basel III, COSO ERM, ISO 31000, NIST CSF, and SOX. Regulatory exam preparation, finding management, and audit trail."},
            {"name": "Cyber Risk", "desc": "Threat detection, vulnerability management, access anomaly monitoring, and incident response tracking. Connects cyber technical indicators to financial risk exposure estimates."},
        ],
        "genie_section": "RiskIQ's Genie AI spans all risk dimensions. Ask 'What is our current credit concentration in commercial real estate?' or 'Show all operational incidents above $1M in the last quarter' and get instant results with supporting data across 290+ pre-trained questions.",
        "business_impact": [
            {"metric": "8", "label": "risk dimensions unified in one platform"},
            {"metric": "5", "label": "regulatory frameworks automated"},
            {"metric": "90 days", "label": "early warning on credit deterioration"},
            {"metric": "40%", "label": "less time spent on compliance reporting"},
        ],
    },
    "telecom": {
        "slug": "telecom",
        "product_name": "TelecomIQ",
        "icon": "&#x1F4E1;",
        "accent": "#14b8a6",
        "hero_tagline": "Network intelligence & customer experience for modern telcos",
        "hero_description": (
            "TelecomIQ delivers end-to-end visibility across subscriber lifecycle, network operations, "
            "and field services for telecom operators managing millions of connections."
        ),
        "the_challenge": (
            "Telecom operators manage some of the most complex infrastructure on the planet &mdash; millions "
            "of subscribers across 4G LTE, 5G, Fiber, and Fixed Wireless networks generating petabytes of "
            "CDR, network performance, and customer interaction data daily. Subscriber churn at 1.8% monthly "
            "means losing over 250,000 customers every month, with each lost subscriber costing $300-500 to "
            "replace. Meanwhile, 5G rollouts require billions in capital expenditure with uncertain ROI timelines. "
            "Fraud costs the industry $38 billion globally. Field operations dispatch 32% unnecessary truck rolls. "
            "And through all of this, subscribers expect 99.99% uptime and instant issue resolution."
        ),
        "the_solution": (
            "TelecomIQ consolidates subscriber data, network telemetry, field service records, and fraud signals "
            "into a unified Databricks Lakehouse. A Subscriber Churn Predictor (AUC 0.88) scores every customer "
            "daily, feeding retention teams with prioritized intervention lists. The Fraud Detector screens "
            "transactions in real-time with 96.2% accuracy, preventing $4.2M in monthly losses. Network operations "
            "get real-time visibility across all technologies with 99.97% uptime monitoring. Field operations "
            "analytics reduce truck roll rates and improve first-call resolution."
        ),
        "metric_deep_dives": [
            {"name": "Subscriber Base", "value": "14.2M", "context": "total active subscribers",
             "explanation": "TelecomIQ tracks the full subscriber base across Consumer, SMB, Enterprise, and Government segments with real-time adds, disconnects, and migration between plan types (Prepaid, Postpaid, Family, Business, IoT). Segmentation analytics reveal that Enterprise accounts represent 8% of subscribers but 34% of revenue."},
            {"name": "Monthly Churn", "value": "1.8%", "context": "subscriber loss rate",
             "explanation": "At 14.2M subscribers, 1.8% monthly churn means 255,600 lost customers every month. Each lost subscriber costs $300-500 to replace through acquisition. TelecomIQ's churn model predicts at-risk subscribers 30 days ahead, considering usage patterns, billing history, service interactions, and network quality in their area. Retention offers are optimized by predicted save probability and customer value."},
            {"name": "ARPU", "value": "$52.30", "context": "average revenue per user",
             "explanation": "ARPU is the fundamental revenue metric for telecom. At $52.30, TelecomIQ decomposes this by segment (Consumer $42, SMB $68, Enterprise $124), plan type, and service mix (voice, data, IoT). Trending ARPU against data usage reveals pricing optimization opportunities."},
            {"name": "Network Uptime", "value": "99.97%", "context": "availability SLA",
             "explanation": "99.97% uptime translates to approximately 13 minutes of total downtime per month. TelecomIQ monitors every cell site, fiber node, and switching center with real-time health scoring. Predictive models identify degradation patterns before they cause outages, while automated incident response workflows reduce MTTR."},
            {"name": "Fraud Blocked (24h)", "value": "229", "context": "daily fraud prevention",
             "explanation": "The Telecom Fraud Detector screens every subscriber event &mdash; SIM swaps, international calls, unusual data patterns &mdash; in real-time. Blocking 229 fraudulent events daily at an average value of $18,300 each prevents $4.2M in monthly losses. The model achieves 96.2% detection with low false-positive rates to avoid blocking legitimate customers."},
            {"name": "5G Coverage", "value": "67%", "context": "network footprint",
             "explanation": "5G coverage tracks the percentage of the service territory with active 5G service. At 67%, TelecomIQ monitors 2.4M IoT devices on the 5G network and tracks migration from 4G. Coverage expansion ROI is modeled by geography, showing which expansion zones will generate the fastest subscriber and revenue uplift."},
        ],
        "page_details": [
            {"name": "Telecom Outcome Hub", "desc": "Executive command center with subscriber health, network status, revenue metrics, and AI morning briefing on overnight network events, churn alerts, and fraud activity."},
            {"name": "Consumer CX & Growth", "desc": "Subscriber lifecycle analytics from acquisition through retention. NPS tracking (42), CSAT monitoring (4.1/5), churn prediction, and personalized offer effectiveness measurement."},
            {"name": "B2B / SMB / Enterprise", "desc": "Business subscriber analytics with contract lifecycle management, SLA compliance tracking, solution profitability, and enterprise account health scoring."},
            {"name": "Network Operations", "desc": "Real-time network health monitoring across 4G LTE, 5G, Fiber, and Fixed Wireless. Cell site performance, capacity planning, and technology migration analytics."},
            {"name": "Field Ops & Energy", "desc": "Field service optimization with truck roll reduction (currently 32%), first-call resolution (78% FCR), MTTR tracking (3.2 hrs), and technician scheduling optimization. Energy cost tracking by site."},
            {"name": "Fraud Prevention", "desc": "Real-time fraud detection across SIM swap, subscription fraud, and usage fraud patterns. 96.2% detection rate with $4.2M monthly prevented losses."},
            {"name": "Cyber Security", "desc": "Network security monitoring, DDoS protection, subscriber data protection, and security incident response tracking. Connects cyber threats to subscriber impact."},
        ],
        "genie_section": "TelecomIQ's Genie AI understands telecom operations natively. Ask 'What is the churn rate for postpaid subscribers in the Northeast region?' or 'Which cell sites had the most dropped calls last week?' across 237+ pre-trained questions.",
        "business_impact": [
            {"metric": "$4.2M/mo", "label": "fraud losses prevented"},
            {"metric": "30 days", "label": "advance churn prediction"},
            {"metric": "14.2M", "label": "subscribers monitored in real-time"},
            {"metric": "32%", "label": "truck roll reduction opportunity"},
        ],
    },
    "customer_support": {
        "slug": "customer-support",
        "product_name": "SupportIQ",
        "icon": "&#x1F3A7;",
        "accent": "#6366f1",
        "hero_tagline": "Contact center intelligence & customer success analytics",
        "hero_description": (
            "SupportIQ transforms contact center data into actionable intelligence &mdash; from intelligent "
            "ticket routing and agent performance to customer health scoring and self-service optimization."
        ),
        "the_challenge": (
            "Contact centers are the frontline of customer experience, yet they operate on outdated metrics and "
            "disconnected systems. With 2,840 daily tickets flowing through Phone, Chat, Email, Social Media, "
            "Self-Service, and Video channels, the volume alone creates triage challenges. Agents spend 8.4 minutes "
            "per handle while only resolving 74.2% of issues on first contact &mdash; meaning one in four customers "
            "must contact support again. Agent attrition runs at 22% annually, constantly draining institutional "
            "knowledge. Quality assurance evaluates only a sample of interactions, missing systemic issues. And the "
            "most critical signal &mdash; customer health as predicted by support patterns &mdash; is invisible "
            "because CRM, ticketing, and telephony systems don't share data."
        ),
        "the_solution": (
            "SupportIQ unifies every support interaction into a single Databricks Lakehouse, creating a complete "
            "picture of customer health derived from support behavior. An Intelligent Ticket Router uses ML to analyze "
            "ticket content, customer history, and agent expertise for optimal routing. A Real-Time Sentiment Analyzer "
            "flags escalation risks during live interactions. A Customer Churn Predictor connects support patterns "
            "to retention probability. Across 342 active agents and 4 global regions, SupportIQ turns the contact "
            "center from a cost center into a customer intelligence engine."
        ),
        "metric_deep_dives": [
            {"name": "CSAT", "value": "4.32 / 5", "context": "customer satisfaction",
             "explanation": "CSAT measures post-interaction satisfaction. At 4.32/5, SupportIQ tracks this by channel, agent, issue category, and resolution path. The data reveals that Chat drives the highest CSAT (4.6) while Phone trails at 4.1 due to longer wait times. More importantly, SupportIQ correlates CSAT scores with downstream retention and upsell probability."},
            {"name": "First Contact Resolution", "value": "74.2%", "context": "resolved on first contact",
             "explanation": "FCR is the single most important operational metric in support &mdash; every repeat contact doubles cost and halves satisfaction. At 74.2%, SupportIQ identifies the 25.8% of issues that require callbacks and diagnoses why: skill gaps, knowledge base gaps, system limitations, or process failures. The intelligent routing model improves FCR by matching tickets to agents with proven expertise in the issue category."},
            {"name": "Average Handle Time", "value": "8.4 min", "context": "per interaction",
             "explanation": "AHT balances efficiency against quality. At 8.4 minutes, SupportIQ tracks handle time by agent, channel, and issue complexity. The key insight is that shorter AHT doesn't always mean better performance &mdash; agents who spend 10 minutes but achieve 90% FCR are more valuable than agents who handle calls in 6 minutes with 60% FCR. SupportIQ models this trade-off."},
            {"name": "NPS", "value": "+47", "context": "net promoter score",
             "explanation": "NPS measures customer loyalty and advocacy. At +47, SupportIQ tracks the relationship between support experiences and NPS trajectory. A single bad support interaction drops individual NPS by an average of 35 points. The sentiment analyzer catches negative experiences in real-time, enabling supervisor intervention before satisfaction damage is permanent."},
            {"name": "SLA Compliance", "value": "92.1%", "context": "within response target",
             "explanation": "SLA compliance measures the percentage of tickets resolved within promised timeframes by tier. At 92.1%, the 7.9% of breaches are concentrated in Tier 3 (Engineering) escalations. SupportIQ provides predictive SLA alerting, flagging tickets likely to breach 2 hours before the deadline so they can be reprioritized or escalated."},
            {"name": "Ticket Backlog", "value": "1,284", "context": "open tickets",
             "explanation": "Backlog size and aging are leading indicators of service degradation. At 1,284 open tickets, SupportIQ monitors backlog growth rate, average age, and composition by category and priority. Trend analysis predicts when backlog will exceed capacity, enabling proactive staffing adjustments."},
        ],
        "page_details": [
            {"name": "Support Command Center", "desc": "Executive dashboard with real-time queue status, CSAT/NPS trends, backlog health, and AI morning briefing on overnight escalations, SLA risks, and staffing gaps."},
            {"name": "Contact Center Analytics", "desc": "Channel-level performance analytics across Phone, Chat, Email, Social, Self-Service, and Video. Volume forecasting, peak hour analysis, and channel migration effectiveness."},
            {"name": "Ticket Intelligence", "desc": "ML-powered ticket categorization, routing optimization, resolution pattern analysis, and knowledge base effectiveness tracking. Identifies common issue clusters and automation opportunities."},
            {"name": "Agent Performance", "desc": "Individual and team performance dashboards covering handle time, FCR, CSAT, quality scores, and productivity. Skills matrix management and coaching opportunity identification."},
            {"name": "Quality Assurance", "desc": "QA scoring analytics (average 87.4), compliance monitoring (96%), calibration tracking, and evaluation trend analysis. Moves QA from sample-based to ML-assisted comprehensive evaluation."},
            {"name": "Customer Health", "desc": "Support-derived customer health scoring combining CSAT, ticket frequency, resolution speed, sentiment, and product usage. The churn predictor identifies accounts at risk based on support interaction patterns."},
            {"name": "Self-Service & AI", "desc": "Chatbot performance analytics, knowledge base utilization, deflection rate tracking, and self-service adoption measurement. Customer Effort Score (CES) of 3.8 tracked by self-service journey."},
        ],
        "genie_section": "SupportIQ's Genie AI understands contact center operations. Ask 'Which agents have the highest FCR rate for billing issues?' or 'What is the average resolution time for Tier 2 escalations this week?' across 40+ pre-trained questions with more being added continuously.",
        "business_impact": [
            {"metric": "2 hrs", "label": "advance SLA breach prediction"},
            {"metric": "342", "label": "agents managed on one platform"},
            {"metric": "6", "label": "support channels unified"},
            {"metric": "22%", "label": "attrition rate tracked with retention signals"},
        ],
    },
    "energy": {
        "slug": "energy",
        "product_name": "EnergyIQ",
        "icon": "&#x26A1;",
        "accent": "#f59e0b",
        "hero_tagline": "Grid operations, asset performance & energy transition analytics",
        "hero_description": (
            "EnergyIQ provides utilities and energy companies with real-time grid visibility, predictive "
            "asset management, and energy transition tracking to maintain reliability while accelerating decarbonization."
        ),
        "the_challenge": (
            "The energy sector is undergoing its most profound transformation in a century. Utilities must "
            "simultaneously maintain grid reliability (customers expect 99.99% uptime), integrate intermittent "
            "renewable sources (currently 31.2% penetration against 50% targets), modernize aging infrastructure "
            "(average T&D asset age exceeds 30 years), and navigate complex rate case proceedings. A single "
            "grid disruption affecting 100,000 customers can cost $50M in emergency response, regulatory penalties, "
            "and reputation damage. Meanwhile, the proliferation of distributed energy resources (solar panels, "
            "battery storage, EV chargers) is turning the traditional one-way grid into a complex two-way network "
            "that existing SCADA systems weren't designed to manage."
        ),
        "the_solution": (
            "EnergyIQ unifies SCADA telemetry, AMI meter reads, DER inverter data, outage management events, "
            "and asset condition scores into a single Databricks Lakehouse. An LSTM-based Grid Load Forecaster "
            "predicts system load with 2.4% MAPE, enabling optimal dispatch. An XGBoost Outage Predictor (AUC 0.91) "
            "identifies failure-prone assets before they cause customer interruptions. A Vegetation Risk Scorer "
            "uses RandomForest models to prioritize tree trimming in right-of-way corridors. Across 5 service "
            "territories and 6 generation types, EnergyIQ delivers the intelligence needed to operate a modern "
            "grid while tracking progress toward clean energy mandates."
        ),
        "metric_deep_dives": [
            {"name": "System Load", "value": "68.4 GW", "context": "current demand",
             "explanation": "Real-time system load is the fundamental grid metric. At 68.4 GW against 92.1 GW peak capacity, EnergyIQ monitors load by service territory and customer class (Residential, Commercial, Industrial, Municipal, EV Charging). The LSTM forecaster predicts load 24 hours ahead with 2.4% MAPE, enabling generation dispatch optimization and demand response program activation."},
            {"name": "Reserve Margin", "value": "14.8%", "context": "capacity buffer",
             "explanation": "Reserve margin measures available generation capacity above peak demand. At 14.8%, EnergyIQ tracks this against NERC reliability standards (typically 15% minimum). Margin is decomposed by generation type &mdash; intermittent renewables reduce effective reserve unless paired with battery storage. The platform models seasonal and weather-driven margin compression scenarios."},
            {"name": "Renewable Penetration", "value": "31.2%", "context": "vs 50% target",
             "explanation": "Renewable penetration measures the percentage of generation from clean sources. At 31.2% against a 50% regulatory target, EnergyIQ tracks solar, wind, hydro, and battery storage output in real-time. The platform models the impact of planned renewable additions on grid stability, curtailment risk, and carbon intensity reduction."},
            {"name": "SAIDI", "value": "108.2 min", "context": "avg interruption duration",
             "explanation": "System Average Interruption Duration Index measures the average total minutes of interruption per customer per year. At 108.2 minutes, EnergyIQ decomposes SAIDI by cause (weather, equipment failure, vegetation, third-party damage), feeder, and territory. The Outage Predictor identifies assets most likely to cause interruptions, enabling preventive maintenance prioritization."},
            {"name": "Peak Demand", "value": "92.1 GW", "context": "seasonal maximum",
             "explanation": "Peak demand drives infrastructure investment requirements. EnergyIQ forecasts seasonal peaks, tracks demand growth trends by territory, and models the impact of EV charging adoption on future peaks. Demand response program analytics show how much peak can be shaved through customer programs."},
            {"name": "T&D Loss", "value": "5.4%", "context": "transmission efficiency",
             "explanation": "Transmission and distribution losses represent energy generated but not delivered to customers. At 5.4%, EnergyIQ identifies loss hotspots by feeder and substation, detecting both technical losses (resistance, transformer inefficiency) and non-technical losses (theft, metering errors). Every 0.1% reduction in losses represents significant revenue recovery."},
        ],
        "page_details": [
            {"name": "Grid Command Center", "desc": "Real-time grid status with system frequency, voltage profiles, load curves, and generation mix. Morning briefing with overnight events, weather-driven demand forecasts, and renewable availability projections."},
            {"name": "Generation & Dispatch", "desc": "Generation unit performance by type (Solar, Wind, Gas, Nuclear, Hydro, Battery), dispatch optimization, fuel cost tracking, and capacity factor analysis. Real-time renewable output vs. forecast."},
            {"name": "Grid Operations", "desc": "Transmission and distribution monitoring with outage tracking, switching operations, power quality metrics, and load balancing across territories. SCADA integration for real-time grid state estimation."},
            {"name": "Customer Programs", "desc": "Demand response enrollment and performance, energy efficiency program tracking, EV charging analytics, time-of-use rate adoption, and net metering management across customer classes."},
            {"name": "Asset Health & Reliability", "desc": "Asset condition scoring, predictive maintenance scheduling, vegetation management prioritization, and reliability metrics (SAIDI/SAIFI/CAIDI). Outage predictor flags high-risk assets."},
            {"name": "Energy Transition", "desc": "Renewable integration tracking, carbon intensity monitoring (720 lbs/MWh), DER management, battery storage analytics, and progress toward clean energy mandates. Models pathway to 50% renewable target."},
            {"name": "Regulatory & Rates", "desc": "Rate case analytics, cost allocation modeling, regulatory filing support, and customer impact analysis. Tracks regulatory lag and revenue requirement recovery."},
        ],
        "genie_section": "EnergyIQ's Genie AI understands grid operations natively. Ask 'What is the current renewable penetration by territory?' or 'Which substations have the highest outage frequency this year?' across 260+ pre-trained energy-specific questions.",
        "business_impact": [
            {"metric": "24 hrs", "label": "ahead load forecasting with 2.4% MAPE"},
            {"metric": "5", "label": "service territories managed on one platform"},
            {"metric": "$50M+", "label": "potential avoided cost per major grid event"},
            {"metric": "31.2%", "label": "renewable penetration tracked toward 50% goal"},
        ],
    },
    "real_estate": {
        "slug": "real-estate",
        "product_name": "PropIQ",
        "icon": "&#x1F3E2;",
        "accent": "#0ea5e9",
        "hero_tagline": "Property analytics, portfolio intelligence & market insights",
        "hero_description": (
            "PropIQ gives real estate investors, operators, and asset managers a unified view of portfolio "
            "performance, leasing velocity, and market dynamics with ML-powered valuations."
        ),
        "the_challenge": (
            "Commercial real estate portfolios generate massive volumes of data &mdash; lease abstracts, rent rolls, "
            "operating statements, market comps, tenant communications, and property condition reports &mdash; yet "
            "most firms still manage this in spreadsheets and disconnected property management systems. A $4.2B "
            "portfolio spanning 8 markets and 6 property types requires real-time visibility into occupancy trends, "
            "rent growth, and cap rate movements to make timely investment and disposition decisions. Tenant churn "
            "is expensive: a vacancy in a Class A office building costs $50-100 per square foot in lost rent and "
            "re-leasing expenses. Market intelligence arrives weeks late through quarterly broker reports when "
            "investment decisions need to be made in days."
        ),
        "the_solution": (
            "PropIQ consolidates all property data into a single Databricks Lakehouse. A GradientBoosting Property "
            "Valuation Model (MAPE 4.8%) supports acquisition decisions and portfolio mark-to-market. A Tenant Churn "
            "Predictor (XGBoost, AUC 0.91) identifies at-risk leases early. A Rent Optimizer (LightGBM, RMSE 1.85) "
            "recommends optimal asking rents by submarket and property type. Across 8 markets, PropIQ delivers the "
            "analytics infrastructure that institutional real estate requires."
        ),
        "metric_deep_dives": [
            {"name": "Portfolio Value", "value": "$4.2B", "context": "total assets under management",
             "explanation": "PropIQ provides real-time portfolio valuation across all property types (Office, Retail, Industrial, Multifamily, Mixed-Use, Data Center) and markets. The ML valuation model updates continuously as market comps, lease events, and economic indicators change, replacing quarterly manual appraisals with dynamic mark-to-market."},
            {"name": "NOI", "value": "$285M", "context": "net operating income",
             "explanation": "NOI is the fundamental measure of property-level profitability &mdash; revenue minus operating expenses before debt service. At $285M, PropIQ decomposes NOI by property, market, and asset class. The platform identifies NOI compression early by tracking expense growth rates against revenue, flagging properties where operating costs are outpacing rent increases."},
            {"name": "Cap Rate", "value": "5.8%", "context": "portfolio yield",
             "explanation": "Capitalization rate (NOI / Property Value) is the universal benchmark for real estate investment returns. At 5.8% against a 6.1% market average, the portfolio trades at a premium reflecting quality and occupancy. PropIQ tracks cap rate compression and expansion by submarket and property type."},
            {"name": "Occupancy", "value": "92.1%", "context": "portfolio-wide",
             "explanation": "Occupancy is tracked in real-time across the portfolio with drill-down by property, floor, and suite. At 92.1% against 87.2% market vacancy, the portfolio outperforms. PropIQ's Tenant Churn Predictor identifies leases at risk 6 months before expiration, giving leasing teams time to negotiate renewals. Tenant retention runs at 82%."},
            {"name": "Avg Rent PSF", "value": "$42.80", "context": "per square foot annually",
             "explanation": "Average rent per square foot tracks pricing power across the portfolio. At $42.80 with 3.4% YoY growth, PropIQ benchmarks rents against market comps by submarket and quality tier. The Rent Optimizer model recommends asking rents that maximize revenue while maintaining competitive occupancy."},
            {"name": "Rent Growth", "value": "+3.4%", "context": "year-over-year",
             "explanation": "Rent growth measures the portfolio's pricing momentum. At +3.4% YoY, PropIQ decomposes growth into mark-to-market on renewals, escalation clauses, and new lease spreads. The platform identifies markets with accelerating vs. decelerating growth to inform capital allocation."},
        ],
        "page_details": [
            {"name": "Portfolio Overview", "desc": "Executive dashboard with total portfolio value, NOI, occupancy, and valuation trends. Morning briefing on lease expirations, market events, and portfolio-level risk indicators."},
            {"name": "Portfolio Analytics", "desc": "Deep-dive into portfolio composition by property type, market, asset class, and investment strategy (Core, Core-Plus, Value-Add, Opportunistic). Return attribution and benchmark comparison."},
            {"name": "Leasing & Occupancy", "desc": "Lease management analytics with expiration tracking, renewal probability, tenant retention analysis, leasing velocity (14.2 days average), and deal pipeline management."},
            {"name": "Market Intelligence", "desc": "Market-level analytics across 8 metros with comp transactions, cap rate trends, vacancy rates, absorption data, and construction pipeline. Competitive set monitoring."},
            {"name": "Acquisitions Pipeline", "desc": "Deal pipeline management with ML-powered valuation, underwriting support, due diligence tracking, and investment committee preparation. Acquisition scoring and ranking."},
            {"name": "Asset Management", "desc": "Property-level operating analytics with budget variance tracking, capex management, vendor performance, and value creation plan monitoring. NOI bridge analysis."},
            {"name": "ESG & Sustainability", "desc": "Energy Star scoring (average 78), LEED certification tracking (42% of portfolio), carbon intensity measurement (8.4), and green capex ROI analysis."},
        ],
        "genie_section": "PropIQ's Genie AI understands commercial real estate natively. Ask 'What is the weighted average lease term remaining for our office portfolio?' or 'Which properties have occupancy below 85%?' across 220+ pre-trained real estate questions.",
        "business_impact": [
            {"metric": "4.8%", "label": "valuation model accuracy (MAPE)"},
            {"metric": "8", "label": "markets tracked with real-time comps"},
            {"metric": "6 months", "label": "advance tenant churn prediction"},
            {"metric": "82%", "label": "tenant retention rate with ML-assisted renewals"},
        ],
    },
    "retail": {
        "slug": "retail",
        "product_name": "RetailIQ",
        "icon": "&#x1F6D2;",
        "accent": "#ec4899",
        "hero_tagline": "Omnichannel retail analytics & commerce intelligence",
        "hero_description": (
            "RetailIQ connects every channel &mdash; in-store, e-commerce, mobile, and marketplace &mdash; into "
            "a unified analytics platform for demand planning, customer intelligence, and margin optimization."
        ),
        "the_challenge": (
            "Retail is in the middle of an omnichannel transformation. With $148M in monthly revenue flowing "
            "through physical stores, e-commerce (34% and growing), mobile apps, and marketplace channels, "
            "the complexity of demand planning has exploded. A stockout in a flagship location costs immediate "
            "sales; excess inventory in an outlet ties up working capital and eventually gets marked down, "
            "destroying margin. Customer behavior is increasingly unpredictable &mdash; a shopper browses on "
            "mobile, buys online, returns in-store. Loss prevention teams face a 1.4% shrinkage rate across "
            "42K daily foot traffic. And through all of this, maintaining a 38.2% gross margin requires precision "
            "pricing, markdown timing, and promotional strategy that most retail analytics tools can't deliver."
        ),
        "the_solution": (
            "RetailIQ unifies POS transactions, e-commerce sessions, foot traffic counts, inventory positions, "
            "and customer profiles into a single Databricks Lakehouse. A Demand Forecast Model (LightGBM, MAPE 5.8%) "
            "predicts demand by SKU and location. A Markdown Optimizer (Bayesian, 8.4% margin lift) times price "
            "reductions for maximum sell-through with minimum margin erosion. A Customer Churn Predictor (XGBoost, "
            "AUC 0.91) identifies at-risk loyalty members. Across 5 regions and 6 departments, RetailIQ gives "
            "retailers the intelligence to compete in an omnichannel world."
        ),
        "metric_deep_dives": [
            {"name": "Revenue MTD", "value": "$148M", "context": "month-to-date sales",
             "explanation": "Monthly revenue is tracked in real-time across all channels with drill-down by store, department, and category. At $148M, RetailIQ decomposes revenue by channel (66% in-store, 34% digital), comp store performance (+4.2% YoY), and new store contribution. The platform identifies revenue risks early by comparing daily actuals against demand forecasts."},
            {"name": "Comp Store Sales", "value": "+4.2%", "context": "same-store YoY growth",
             "explanation": "Comp store sales growth strips out new store openings to show organic performance. At +4.2%, RetailIQ tracks comps by store format (Flagship, Mall, Outlet, Express, Dark Store), region, and department. This metric is the primary indicator Wall Street uses to evaluate retail health, making real-time visibility essential for investor relations and operational planning."},
            {"name": "Gross Margin", "value": "38.2%", "context": "revenue minus COGS",
             "explanation": "Gross margin is the single most important profitability metric in retail. At 38.2%, RetailIQ tracks margin by category, vendor, and channel &mdash; revealing that e-commerce runs at 42% margin (no rent) while outlet stores operate at 28% (deep discounts). The Markdown Optimizer protects margin by timing price reductions to maximize revenue capture while minimizing margin erosion."},
            {"name": "Conversion Rate", "value": "3.4%", "context": "traffic to purchase",
             "explanation": "Conversion rate connects customer traffic to actual purchases. At 3.4%, RetailIQ tracks conversion by channel, store, hour, and department. In-store conversion runs at 24% (foot traffic to transaction) while e-commerce sits at 3.4% (sessions to orders). The platform correlates conversion with staffing levels, merchandising changes, and promotional activity."},
            {"name": "E-Commerce Share", "value": "34%", "context": "digital revenue mix",
             "explanation": "Digital revenue share tracks the omnichannel transition. At 34% and growing, RetailIQ monitors the shift from physical to digital, tracking cannibalization vs. incremental growth. Fulfillment analytics cover the 4.6-hour average fulfillment time, 8.9% return rate, and BOPIS (buy online, pick up in store) adoption. Digital margin profiles differ from physical, making mix shifts strategically significant."},
            {"name": "Shrinkage", "value": "1.4%", "context": "inventory loss rate",
             "explanation": "Shrinkage costs the US retail industry $100+ billion annually. At 1.4%, RetailIQ's loss prevention analytics decompose shrink by store, department, and cause (theft, vendor fraud, administrative error, spoilage). Anomaly detection flags unusual POS patterns, void rates, and inventory discrepancies that indicate organized retail crime."},
        ],
        "page_details": [
            {"name": "Retail Command Center", "desc": "Executive dashboard with real-time revenue, comp store performance, margin health, and AI morning briefing on overnight e-commerce activity, inventory alerts, and store-level anomalies."},
            {"name": "Merchandising & Assortment", "desc": "Category management analytics with sell-through rates, inventory depth, markdown effectiveness, and vendor performance. Assortment optimization by store cluster and season."},
            {"name": "Customer Analytics", "desc": "Customer segmentation across Loyalty tiers (Platinum, Gold, Silver, Casual, New), RFM analysis, basket analysis, and churn prediction. 58% loyalty penetration tracking with lifetime value by segment."},
            {"name": "Store Operations", "desc": "Store-level P&L, labor scheduling optimization, traffic pattern analysis, and associate productivity metrics. 42K daily foot traffic tracked with conversion correlation."},
            {"name": "Supply Chain & Inventory", "desc": "Demand forecasting by SKU/location, inventory position monitoring, fill rate tracking (96.4%), and stockout prevention (3.1% rate). 28.4 days of supply managed across distribution network."},
            {"name": "E-Commerce & Digital", "desc": "Digital analytics covering session metrics, conversion funnels, search performance, personalization effectiveness, and fulfillment operations. 34% digital revenue share tracked in real-time."},
            {"name": "Loss Prevention & Shrink", "desc": "Shrinkage analytics by cause and location, POS exception monitoring, high-risk transaction flagging, and organized retail crime detection. 1.4% shrink rate decomposed by driver."},
        ],
        "genie_section": "RetailIQ's Genie AI understands retail operations natively. Ask 'What is the sell-through rate for winter apparel in the Northeast?' or 'Which stores have shrinkage above 2%?' across 240+ pre-trained retail questions.",
        "business_impact": [
            {"metric": "8.4%", "label": "margin lift from markdown optimization"},
            {"metric": "5.8%", "label": "demand forecast accuracy (MAPE)"},
            {"metric": "5 regions", "label": "managed from one unified platform"},
            {"metric": "58%", "label": "loyalty penetration driving repeat revenue"},
        ],
    },
}
