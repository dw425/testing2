#!/usr/bin/env python3
"""
Generate rich marketing HTML product pages for each Blueprint-IQ vertical.
Usage: python3 docs/generate_pages.py
"""
import yaml
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = ROOT / "config"
OUT_DIR = ROOT / "docs" / "verticals"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# FA icon -> simple SVG map
ICON_SVG = {
    "fa-building-columns": "M3 21h18M3 10h18M5 6l7-3 7 3M4 10v11M8 10v11M12 10v11M16 10v11M20 10v11",
    "fa-chart-line": "M3 17l4-4 4 4 4-6 4 2",
    "fa-chart-pie": "M12 2a10 10 0 0110 10H12V2zM12 12L2.05 14A10 10 0 0012 2v10z",
    "fa-shield-halved": "M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z",
    "fa-gavel": "M2 21h20M5 17l2-2M14 6l2-2M8 14l6-6M6 12l6-6 2 2-6 6z",
    "fa-user-shield": "M12 12a4 4 0 100-8 4 4 0 000 8zm-6 8a6 6 0 0112 0H6zm14-4l2 2 4-4",
    "fa-gears": "M12 15a3 3 0 100-6 3 3 0 000 6z",
    "fa-gamepad": "M6 12h4M8 10v4M15 11h.01M18 13h.01M17.32 5H6.68a4 4 0 00-3.978 3.59L2 14a4 4 0 008 0h4a4 4 0 008 0l-.702-5.41A4 4 0 0017.32 5z",
    "fa-users-viewfinder": "M16 21v-2a4 4 0 00-4-4H6a4 4 0 00-4-4v2m14 0a3 3 0 100-6 3 3 0 000 6z",
    "fa-rocket": "M4.5 16.5L3 21l4.5-1.5M12 15l-3-3m0 0l5-7 7 5-7 5zM9 12l-6 6",
}

def icon_svg(fa_name, accent="#6b7280"):
    """Return inline SVG for a FA icon name, or a default chart icon."""
    path = ICON_SVG.get(fa_name, "M3 17l4-4 4 4 4-6 4 2")
    return (f'<svg class="w-6 h-6 flex-shrink-0" fill="none" stroke="{accent}" '
            f'stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" '
            f'stroke-linejoin="round" d="{path}"/></svg>')


# ── Rich marketing copy per vertical ────────────────────────────────────────
VERTICALS = {
    "financial_services": {
        "slug": "financial-services",
        "product_name": "FinServIQ",
        "icon": "&#x1F3E6;",
        "accent": "#1d4ed8",
        "hero_tagline": "Unified intelligence across Banking, Capital Markets & Insurance",
        "hero_description": (
            "FinServIQ transforms fragmented financial data into a single pane of glass for "
            "portfolio analytics, fraud detection, risk management, and regulatory compliance. "
            "Built on Databricks Lakehouse, it delivers real-time insights that drive alpha, "
            "reduce losses, and accelerate reporting."
        ),
        "the_challenge": (
            "Financial institutions operate in one of the most data-intensive industries on Earth. "
            "A mid-size bank processes over 12 million transactions daily across retail, commercial, "
            "and investment lines. Each transaction must be screened for fraud, assessed for credit risk, "
            "and recorded for regulatory compliance &mdash; often across disconnected systems that were "
            "built decades apart. Capital markets desks need sub-second risk calculations while compliance "
            "teams need audit-ready reporting on demand. Insurance underwriters need real-time loss ratio "
            "visibility while claims adjusters need fraud signals at the point of triage. The result is "
            "a fragmented data landscape where critical signals are lost between silos, decisions are made "
            "on stale data, and regulatory reporting consumes thousands of analyst hours per quarter."
        ),
        "the_solution": (
            "FinServIQ unifies these workstreams on a single Databricks Lakehouse, creating one source of "
            "truth that spans banking operations, capital markets trading, insurance underwriting, and "
            "enterprise compliance. Raw transaction feeds, market data, and claims records flow through a "
            "medallion architecture &mdash; Bronze for raw ingestion, Silver for enrichment and ML scoring, "
            "Gold for business-ready analytics. ML models run in real-time against this unified data layer: "
            "a Transaction Fraud Detector screens every payment, a Credit Default Predictor scores every "
            "loan, and a Claims Fraud Detector flags suspicious patterns before payouts are approved. "
            "The Genie AI assistant lets any stakeholder &mdash; from a portfolio manager to a compliance "
            "officer &mdash; ask natural-language questions and get instant, data-backed answers."
        ),
        "metric_deep_dives": [
            {
                "name": "Alpha Generation",
                "value": "+180 bps",
                "context": "vs. benchmark",
                "explanation": (
                    "Alpha measures the excess return generated above a benchmark index. At +180 basis points, "
                    "FinServIQ-equipped portfolio teams are outperforming their benchmarks by 1.8% &mdash; a "
                    "significant edge in institutional asset management where even 50 bps of consistent alpha "
                    "can differentiate a top-quartile fund. This is driven by real-time factor attribution, "
                    "sector rotation signals, and ML-powered anomaly detection on trading patterns that surface "
                    "opportunities before they become consensus."
                ),
            },
            {
                "name": "Value at Risk (95%)",
                "value": "$47M",
                "context": "daily portfolio exposure",
                "explanation": (
                    "VaR quantifies the maximum expected loss over a one-day horizon at a 95% confidence level. "
                    "A $47M daily VaR means that on 19 out of 20 trading days, the portfolio will not lose more "
                    "than this amount. FinServIQ computes VaR continuously rather than in overnight batch &mdash; "
                    "giving risk managers intraday visibility into exposure shifts from market movements, new "
                    "positions, or counterparty events. This real-time capability is critical for firms subject to "
                    "Basel III market risk capital requirements."
                ),
            },
            {
                "name": "Fraud Detection Rate",
                "value": "96.2%",
                "context": "ML model accuracy",
                "explanation": (
                    "Transaction fraud costs the global banking industry over $30 billion annually. FinServIQ's "
                    "fraud detection model achieves 96.2% accuracy on a stream of 12.5 million daily transactions, "
                    "blocking an average of 847 fraudulent events per day. The model uses gradient-boosted decision "
                    "trees trained on transaction velocity, geolocation, merchant category, device fingerprinting, "
                    "and behavioral biometrics. SHAP explainability is built in, so fraud analysts can see exactly "
                    "why a transaction was flagged &mdash; critical for regulatory defensibility."
                ),
            },
            {
                "name": "Net Interest Margin",
                "value": "3.1%",
                "context": "core banking profitability",
                "explanation": (
                    "NIM is the fundamental measure of bank profitability &mdash; the spread between interest earned "
                    "on loans and interest paid on deposits. At 3.1%, FinServIQ tracks NIM in real-time across retail "
                    "banking, commercial lending, and wealth management divisions. The dashboard decomposes NIM by "
                    "product line, vintage, and region, allowing treasury teams to identify margin compression early "
                    "and adjust pricing strategies before quarterly earnings pressure."
                ),
            },
            {
                "name": "Combined Ratio",
                "value": "96.4%",
                "context": "insurance underwriting profitability",
                "explanation": (
                    "The combined ratio measures whether an insurance operation is profitable from underwriting alone. "
                    "A ratio below 100% means the insurer earns more in premiums than it pays in claims and expenses. "
                    "At 96.4%, the insurance division is generating a 3.6% underwriting profit. FinServIQ breaks this "
                    "down by line of business &mdash; Auto, Home, Life, Commercial, Health &mdash; and uses ML-based "
                    "claims fraud detection (catching 8.2% fraud rate) to keep loss ratios in check."
                ),
            },
            {
                "name": "STP Rate",
                "value": "94%",
                "context": "straight-through processing",
                "explanation": (
                    "Straight-through processing measures the percentage of transactions that complete without manual "
                    "intervention. A 94% STP rate means only 6% of trades, payments, or claims require human touch &mdash; "
                    "reducing operational cost per transaction by up to 80%. FinServIQ monitors STP rates across settlement, "
                    "clearing, and reconciliation workflows, flagging automation breakdowns in real-time."
                ),
            },
        ],
        "page_details": [
            {"name": "Capital Markets Hub", "desc": "Executive command center with real-time P&L, exposure heatmaps, morning briefing with overnight market moves, and cross-desk performance attribution. Designed for CIOs and desk heads who need a single view of the trading floor."},
            {"name": "Investment Analytics & Alpha", "desc": "Portfolio construction analytics including factor decomposition, sector rotation signals, benchmark tracking, and alpha attribution by strategy. Integrates with ML-based opportunity scoring to surface high-conviction trade ideas."},
            {"name": "Trading & Advisory", "desc": "Flow analytics, execution quality metrics, client activity heatmaps, and advisory performance tracking. Helps sales traders and advisors prioritize client engagement based on revenue potential and activity patterns."},
            {"name": "Risk Management", "desc": "Multi-dimensional risk dashboard covering market risk (VaR, Greeks), credit risk (PD/LGD/EAD), and counterparty exposure. Stress testing scenarios run on-demand against the live portfolio with results in minutes, not hours."},
            {"name": "Regulatory & Compliance", "desc": "Automated regulatory reporting for Basel III capital requirements, SOX controls testing, and AML transaction monitoring. Reduces quarterly reporting cycles from weeks to days with pre-built report templates and audit trails."},
            {"name": "Fraud & Cybersecurity", "desc": "Real-time fraud screening across card transactions, wire transfers, and ACH payments. The ML fraud model processes 12.5M transactions/day with SHAP-powered explainability for every flagged event. Cyber threat monitoring tracks login anomalies and data exfiltration patterns."},
            {"name": "Operations & Efficiency", "desc": "Back-office operations analytics covering settlement fails, reconciliation breaks, and STP rates. Identifies automation opportunities and tracks operational risk events across the trade lifecycle."},
        ],
        "genie_section": (
            "FinServIQ includes over 100 pre-trained Genie AI questions spanning portfolio analytics, fraud investigation, "
            "credit risk assessment, and regulatory reporting. A portfolio manager can ask 'What is our current VaR by desk?' "
            "and get an instant answer with supporting SQL. A compliance officer can ask 'Show me all AML alerts above $100K "
            "in the last 30 days' and receive a filterable dataset. Genie understands the financial services data model natively "
            "&mdash; it knows the difference between a limit order and a market order, between a debit and a credit, between "
            "a premium and a deductible."
        ),
        "business_impact": [
            {"metric": "60%", "label": "reduction in regulatory reporting time"},
            {"metric": "847+", "label": "fraudulent transactions blocked daily"},
            {"metric": "$24.5B", "label": "in assets under management visibility"},
            {"metric": "94%", "label": "straight-through processing rate"},
        ],
    },
    "gaming": {
        "slug": "gaming",
        "product_name": "GamingIQ",
        "icon": "&#x1F3AE;",
        "accent": "#06b6d4",
        "hero_tagline": "Player intelligence that drives retention, revenue & growth",
        "hero_description": (
            "GamingIQ gives studios and publishers a 360-degree view of player behavior, monetization, "
            "and live operations. From cohort retention analysis to real-time economy monitoring, it "
            "connects every signal &mdash; acquisition, engagement, spend, and churn &mdash; into "
            "actionable intelligence powered by Databricks and ML."
        ),
        "the_challenge": (
            "The gaming industry operates on razor-thin margins between viral success and costly failure. "
            "A title with 2.4 million daily active users generates terabytes of behavioral telemetry every "
            "day &mdash; session starts, level completions, IAP events, ad impressions, social interactions, "
            "and crash logs. Studios need to understand not just what players are doing, but why they leave. "
            "Player acquisition costs have risen 40% year-over-year, making retention the single most important "
            "lever for profitability. Yet most studios still rely on fragmented analytics tools that show "
            "surface-level metrics without connecting acquisition spend to lifetime value, or game economy "
            "health to churn risk. By the time a player churns, the revenue opportunity is already lost."
        ),
        "the_solution": (
            "GamingIQ consolidates every player signal into a unified lakehouse &mdash; from first install "
            "through years of engagement. The platform tracks players across three live titles (Stellar Conquest, "
            "Shadow Realms, Velocity Rush) and six global regions, segmenting users into behavioral cohorts "
            "(Whale, Dolphin, Minnow, Free-to-Play) with ML-driven predictions for churn, spend propensity, "
            "and lifetime value. Real-time dashboards monitor the in-game economy (currency sinks vs. sources, "
            "item pricing elasticity, IAP conversion funnels) while the UA analytics module connects every "
            "dollar of acquisition spend to downstream revenue through ROAS attribution. Live ops teams get "
            "instant visibility into event performance, server health, and content engagement across all titles."
        ),
        "metric_deep_dives": [
            {
                "name": "Player LTV",
                "value": "$47.80",
                "context": "average lifetime value",
                "explanation": (
                    "Lifetime value is the total revenue a player generates from install to churn. At $47.80, "
                    "GamingIQ benchmarks and forecasts LTV across segments and titles. Whale players (top 2% of "
                    "spenders) drive 45% of revenue with LTVs exceeding $800, while the long tail of minnow and "
                    "free-to-play users contribute through ad impressions and social virality. GamingIQ's LTV "
                    "Predictor model estimates 90-day projected value within 48 hours of install, allowing UA "
                    "teams to optimize bidding and creative targeting in real-time."
                ),
            },
            {
                "name": "D7 Retention",
                "value": "41%",
                "context": "day-7 return rate",
                "explanation": (
                    "Day-7 retention is the gold standard early indicator of product-market fit in gaming. A 41% "
                    "D7 rate means more than 4 in 10 players return a full week after install &mdash; well above "
                    "the 25-30% industry median for mobile. GamingIQ tracks retention curves by cohort, source, "
                    "device, region, and first-session behavior, identifying exactly where and why players drop off. "
                    "The churn prediction model (AUC 0.91) flags at-risk players 7 days before they leave, giving "
                    "live ops teams a window to intervene with targeted offers, content, or re-engagement campaigns."
                ),
            },
            {
                "name": "ARPDAU",
                "value": "$0.118",
                "context": "avg revenue per daily active user",
                "explanation": (
                    "ARPDAU combines IAP revenue (72%), ad revenue (18%), and subscription revenue (10%) into a "
                    "single per-user metric. At $0.118, every 100K DAU generates $11,800 in daily revenue. GamingIQ "
                    "decomposes ARPDAU by monetization channel, player segment, and title &mdash; revealing that "
                    "Stellar Conquest's ARPDAU is 2.1x higher than Shadow Realms due to its battle pass structure. "
                    "This insight drives game design decisions and cross-title monetization strategy."
                ),
            },
            {
                "name": "ROAS",
                "value": "1.52x",
                "context": "return on ad spend",
                "explanation": (
                    "With $420K in monthly UA spend across Google, Meta, TikTok, Apple Search Ads, and cross-promo "
                    "channels, ROAS measures every dollar's return. A 1.52x ROAS means each dollar generates $1.52 "
                    "in attributed revenue. GamingIQ connects UA spend to downstream LTV through multi-touch "
                    "attribution, showing that TikTok creatives drive the highest D7 retention while Google delivers "
                    "the highest initial volume. This data powers real-time bid optimization and budget reallocation."
                ),
            },
            {
                "name": "DAU",
                "value": "2.4M",
                "context": "daily active users",
                "explanation": (
                    "Daily active users is the heartbeat of a live-service game. GamingIQ tracks 2.4M DAU with "
                    "847K concurrent peak across all titles and regions. The platform monitors DAU trends by hour, "
                    "correlating drops with server issues, content gaps, or competitive launches. The 68% D1 "
                    "retention rate means strong first-day hooks, while the 22% D30 retention shows healthy "
                    "long-term engagement loops."
                ),
            },
            {
                "name": "Contribution Margin",
                "value": "68.4%",
                "context": "net profitability after UA & platform fees",
                "explanation": (
                    "Contribution margin strips out UA costs, platform fees (Apple/Google 30%), and server costs to "
                    "show true per-title profitability. At 68.4%, the portfolio is generating strong margin after "
                    "variable costs. GamingIQ tracks this at the title, region, and cohort level &mdash; revealing "
                    "that mature cohorts (90+ days) have near-zero UA cost and 85%+ contribution margin, making "
                    "retention investment the highest-ROI activity in the business."
                ),
            },
        ],
        "page_details": [
            {"name": "Player Experience Hub", "desc": "Executive dashboard with real-time DAU, revenue, retention curves, and morning briefing on overnight performance. Shows cross-title portfolio health with drill-down into each game's player funnel and economy metrics."},
            {"name": "Know the Player", "desc": "Deep player segmentation with behavioral clustering, spend propensity scoring, and session-level journey analysis. Answers the fundamental question: who are our players, what do they value, and how do we keep them?"},
            {"name": "Grow the Playerbase", "desc": "User acquisition analytics connecting spend to installs, installs to D1/D7/D30 retention, and retention to LTV. ROAS tracking by channel, creative, and geo with budget optimization recommendations."},
            {"name": "Grow Revenue", "desc": "Monetization intelligence covering IAP funnels, ad yield by placement, subscription conversion, and battle pass engagement. Price elasticity testing and offer targeting by player segment."},
            {"name": "Build Games", "desc": "Development analytics tracking milestone completion, build pipeline health, QA bug resolution rates, and feature adoption after release. Connects development velocity to player-facing outcomes."},
            {"name": "Live Ops", "desc": "Real-time event monitoring, A/B test results, content rotation performance, and player engagement with live events. Server health metrics, matchmaking quality, and latency tracking across all regions."},
            {"name": "Efficient Ops", "desc": "Infrastructure cost analytics, cloud spend optimization, support ticket analysis, and operational efficiency metrics. Tracks cost-per-DAU and identifies opportunities to reduce server and support overhead."},
        ],
        "genie_section": (
            "GamingIQ's Genie AI understands gaming-native concepts out of the box. Ask 'What is D7 retention for "
            "Stellar Conquest players acquired through TikTok last month?' and get an instant cohort analysis. Ask "
            "'Which player segment has the highest churn risk this week?' and get a ranked list with confidence scores. "
            "The assistant has been trained on 260+ gaming-specific questions covering retention, monetization, UA, "
            "economy health, live ops, and development velocity &mdash; no SQL required."
        ),
        "business_impact": [
            {"metric": "7 days", "label": "advance warning on player churn"},
            {"metric": "2.4M", "label": "daily active users tracked in real-time"},
            {"metric": "$420K/mo", "label": "UA spend optimized through ROAS attribution"},
            {"metric": "3 titles", "label": "managed from a single unified platform"},
        ],
    },
    "hls": {
        "slug": "healthcare",
        "product_name": "HLSIQ",
        "icon": "&#x1F3E5;",
        "accent": "#10b981",
        "hero_tagline": "Outcome-driven intelligence for Providers, Payers & Pharma",
        "hero_description": (
            "HLSIQ unifies clinical, operational, and financial data to improve patient outcomes, "
            "reduce readmissions, and optimize resource utilization across the healthcare continuum."
        ),
        "the_challenge": (
            "Healthcare organizations sit on vast stores of clinical, claims, and operational data &mdash; "
            "yet struggle to turn it into timely, actionable intelligence. Emergency departments face 34-minute "
            "average wait times while bed utilization hovers at 87%, indicating both capacity constraints and "
            "flow inefficiencies. Readmission rates of 8.2% carry massive financial penalties under CMS programs, "
            "and health plans lose millions annually to fraud, waste, and abuse that slips through rule-based "
            "screening systems. BioPharma companies spend $2.6B on average to bring a drug to market, yet "
            "clinical trial enrollment sits at just 78% of target. The common thread is disconnected data: "
            "EHRs don't talk to claims systems, supply chain data lives in separate ERPs, and quality measures "
            "are computed quarterly when they should be monitored daily."
        ),
        "the_solution": (
            "HLSIQ breaks these silos by unifying patient records, claims data, supply chain feeds, and quality "
            "metrics on a single Databricks Lakehouse. The platform serves four sub-verticals &mdash; Providers, "
            "Health Plans, BioPharma, and MedTech &mdash; each with domain-specific analytics, ML models, and "
            "Genie AI capabilities. A Readmission Risk Predictor (AUC 0.89) scores every discharged patient, "
            "enabling care coordinators to intervene before costly re-hospitalizations. An FWA Detection Model "
            "screens claims in real-time, catching anomalous billing patterns that rule-based systems miss. "
            "Across three facilities and six departments, HLSIQ delivers the operational intelligence needed "
            "to reduce wait times, improve bed turnover, and drive Star Rating improvements."
        ),
        "metric_deep_dives": [
            {
                "name": "Bed Utilization",
                "value": "87.3%",
                "context": "capacity efficiency",
                "explanation": (
                    "Bed utilization measures the percentage of available beds occupied at any given time. While 87.3% "
                    "suggests strong demand, the real insight is in the variance: ICU runs at 94% (dangerously tight) "
                    "while medical/surgical wards operate at 78%. HLSIQ tracks utilization by department, floor, and "
                    "hour &mdash; enabling patient flow teams to predict bottlenecks 6 hours ahead and trigger early "
                    "discharge planning, transfer coordination, or temporary bed expansion before capacity crises hit."
                ),
            },
            {
                "name": "30-Day Readmission Rate",
                "value": "8.2%",
                "context": "all-cause readmissions",
                "explanation": (
                    "Hospital readmissions within 30 days cost the US healthcare system over $26 billion annually. "
                    "CMS penalizes hospitals with excess readmission rates through the Hospital Readmissions Reduction "
                    "Program, making this metric directly tied to reimbursement. HLSIQ's ML model scores every "
                    "discharged patient on readmission probability, factoring in diagnosis, comorbidities, social "
                    "determinants, medication complexity, and prior utilization. High-risk patients are automatically "
                    "flagged for post-discharge follow-up, reducing avoidable readmissions by targeting interventions "
                    "where they matter most."
                ),
            },
            {
                "name": "Star Rating",
                "value": "4.2 / 5",
                "context": "CMS quality composite",
                "explanation": (
                    "CMS Star Ratings directly impact Medicare Advantage reimbursement and patient choice. Each half-star "
                    "improvement can represent millions in bonus payments. HLSIQ tracks all 40+ underlying quality measures "
                    "in real-time rather than waiting for annual CMS reporting cycles, allowing quality teams to identify "
                    "and address measure gaps months before they impact ratings."
                ),
            },
            {
                "name": "FWA Detection Rate",
                "value": "4.7%",
                "context": "fraud, waste & abuse identified",
                "explanation": (
                    "Industry estimates suggest 3-10% of healthcare spending is lost to fraud, waste, and abuse. HLSIQ's "
                    "FWA model screens claims data in real-time, identifying upcoding patterns, unbundling schemes, phantom "
                    "billing, and duplicate claims. At a 4.7% detection rate on a large health plan book, this translates "
                    "to tens of millions in annual savings &mdash; far exceeding what rule-based systems catch."
                ),
            },
            {
                "name": "ED Wait Time",
                "value": "34 min",
                "context": "door-to-provider average",
                "explanation": (
                    "Emergency department wait time is a critical patient experience metric and a leading indicator of "
                    "operational efficiency. At 34 minutes, HLSIQ tracks this in real-time by facility and acuity level, "
                    "correlating wait times with staffing levels, arrival patterns, and bed availability. Predictive models "
                    "forecast ED volume 4 hours ahead, enabling proactive staffing adjustments."
                ),
            },
            {
                "name": "Medical Loss Ratio",
                "value": "84.2%",
                "context": "claims cost to premium ratio",
                "explanation": (
                    "MLR measures the percentage of premium revenue spent on medical claims. The ACA requires a minimum "
                    "80% MLR for individual plans and 85% for large group. At 84.2%, the health plan is operating within "
                    "regulatory compliance while maintaining margin. HLSIQ decomposes MLR by plan type (HMO, PPO, Medicare "
                    "Advantage, Medicaid), therapeutic area, and provider network to identify cost drivers."
                ),
            },
        ],
        "page_details": [
            {"name": "HLS Outcome Hub", "desc": "Executive dashboard showing patient volume, quality metrics, financial performance, and AI-generated morning briefing on overnight census changes, quality alerts, and operational flags across all facilities."},
            {"name": "Healthcare Operations", "desc": "Real-time patient flow analytics covering ED throughput, bed management, surgical scheduling, and discharge planning. Tracks door-to-provider times, length of stay by DRG, and staffing ratio compliance."},
            {"name": "Network & Quality", "desc": "Provider network performance with Star Rating tracking, HEDIS measure compliance, provider profiling, and narrow network analysis. Identifies high-value providers and quality improvement opportunities."},
            {"name": "BioPharma Intelligence", "desc": "Clinical trial enrollment tracking, drug pipeline analytics, HCP engagement scoring, and manufacturing yield monitoring. Connects R&D investment to market outcomes across therapeutic areas."},
            {"name": "Supply Chain", "desc": "Medical supply chain visibility covering inventory levels, procurement costs, vendor performance, and demand forecasting for critical items (PPE, pharmaceuticals, surgical supplies). Tracks par levels by department."},
            {"name": "MedTech & Surgical", "desc": "Medical device pipeline analytics, surgical outcome tracking, device utilization rates, and post-market surveillance. Connects device performance data to patient outcomes for regulatory and quality reporting."},
            {"name": "Patient Outcomes", "desc": "Population health analytics with risk stratification, care gap identification, chronic disease management tracking, and social determinants of health integration. The readmission ML model scores every discharge."},
        ],
        "genie_section": (
            "HLSIQ's Genie AI speaks the language of healthcare natively. Ask 'What is our current readmission rate for "
            "heart failure patients?' and get an instant analysis by facility, payer, and discharge disposition. Ask "
            "'Which departments are over 90% bed utilization right now?' and get a real-time capacity view. The assistant "
            "understands clinical terminology, quality measure definitions, and regulatory reporting requirements across "
            "260+ pre-trained healthcare questions."
        ),
        "business_impact": [
            {"metric": "6 hrs", "label": "advance prediction of capacity bottlenecks"},
            {"metric": "$26B", "label": "industry readmission cost HLSIQ helps reduce"},
            {"metric": "40+", "label": "CMS quality measures tracked in real-time"},
            {"metric": "3", "label": "facilities monitored from one unified platform"},
        ],
    },
    "manufacturing": {
        "slug": "manufacturing",
        "product_name": "MakerIQ",
        "icon": "&#x1F3ED;",
        "accent": "#8b5cf6",
        "hero_tagline": "Production analytics, predictive maintenance & supply chain intelligence",
        "hero_description": (
            "MakerIQ transforms raw sensor telemetry, ERP data, and quality inspections into real-time "
            "operational intelligence. With ML-powered anomaly detection, demand forecasting, and SHAP "
            "explainability, it helps manufacturers maximize OEE and eliminate unplanned downtime."
        ),
        "the_challenge": (
            "Modern manufacturing generates more data than any other industry &mdash; a single CNC machine "
            "produces thousands of telemetry readings per second across vibration, temperature, spindle RPM, "
            "and tool wear. Yet most plants still rely on threshold-based alerts that either fire too late "
            "(after the machine has already failed) or too often (creating alert fatigue that gets ignored). "
            "Unplanned downtime costs manufacturers an estimated $50 billion annually. Meanwhile, supply chain "
            "volatility means demand forecasts made 90 days ago are already obsolete, leading to either excess "
            "inventory (tying up working capital) or stockouts (stopping production lines). Quality teams catch "
            "defects at end-of-line inspection when the cost of scrap is already incurred, instead of at the "
            "source where root causes can be addressed in real-time."
        ),
        "the_solution": (
            "MakerIQ is the most fully-realized vertical in the Blueprint IQ platform, with a complete "
            "end-to-end implementation from raw data ingestion to production ML models. Auto Loader streams "
            "IoT telemetry from CNC machines into Bronze tables, where it's enriched in Silver with ML-scored "
            "anomaly detection (GradientBoosting on vibration, temperature, spindle RPM, tool wear, and feed "
            "rate). A Prophet-based demand forecaster generates 90-day inventory projections per site and "
            "component. SHAP explainability shows operators exactly which sensor reading drove each anomaly flag. "
            "Gold tables aggregate production KPIs, inventory forecasts, and model health metrics into "
            "dashboard-ready views across 5 global facilities."
        ),
        "metric_deep_dives": [
            {
                "name": "Overall Equipment Effectiveness",
                "value": "84.7%",
                "context": "world-class target: 85%+",
                "explanation": (
                    "OEE is the gold standard for manufacturing productivity, combining Availability (uptime), "
                    "Performance (speed), and Quality (yield) into a single percentage. At 84.7%, MakerIQ-equipped "
                    "plants are approaching world-class levels (85%+). The platform decomposes OEE by facility, "
                    "production line, shift, and machine &mdash; revealing that the Austin Assembly plant runs at "
                    "89% OEE while Shanghai Precision Works lags at 79% due to higher changeover times. This "
                    "granularity enables targeted improvement programs rather than blanket initiatives."
                ),
            },
            {
                "name": "First Pass Yield",
                "value": "96.3%",
                "context": "quality rate",
                "explanation": (
                    "FPY measures the percentage of units that pass quality inspection without rework. Each point "
                    "of FPY improvement can represent millions in saved scrap and rework costs across high-volume "
                    "production. MakerIQ's Quality Defect Predictor uses XGBoost trained on raw material lot scores, "
                    "machine age, ambient conditions, operator experience, and tool wear to predict defects before "
                    "they occur &mdash; shifting quality from detection to prevention. The current defect rate of "
                    "620 PPM is tracked by component, process step, and shift."
                ),
            },
            {
                "name": "On-Time Delivery",
                "value": "93%",
                "context": "supply chain reliability",
                "explanation": (
                    "On-time delivery measures the percentage of customer orders shipped within the promised window. "
                    "At 93%, MakerIQ tracks this across the full supply chain from supplier lead times (14.2 days "
                    "average) through production scheduling to final shipment. The demand forecast model (LightGBM, "
                    "MAPE 6.2%) predicts component needs 90 days out, while inventory turn tracking at 8.6x "
                    "ensures working capital isn't trapped in excess stock."
                ),
            },
            {
                "name": "Unplanned Downtime",
                "value": "3.2%",
                "context": "target: below 5%",
                "explanation": (
                    "Unplanned downtime is the single most expensive operational failure in manufacturing. Even "
                    "a 1% reduction in downtime across a 5-plant operation can save millions annually. MakerIQ's "
                    "Equipment Anomaly Detector runs IsolationForest models on real-time sensor streams, identifying "
                    "failure signatures 24-72 hours before breakdown. SHAP values tell maintenance teams exactly "
                    "which sensor (vibration, temperature, tool wear) is driving the anomaly, enabling precision "
                    "maintenance instead of reactive repair."
                ),
            },
            {
                "name": "Defect Rate",
                "value": "620 PPM",
                "context": "parts per million",
                "explanation": (
                    "PPM defect rate is the universal quality benchmark in manufacturing. 620 PPM means 620 defective "
                    "parts per million produced. MakerIQ tracks PPM by facility, product line, and process step, with "
                    "Cpk analysis (average 1.54) showing process capability relative to specification limits. Real-time "
                    "tolerance monitoring catches drift before it becomes out-of-spec product."
                ),
            },
            {
                "name": "Inventory Turns",
                "value": "8.6x",
                "context": "annual turnover",
                "explanation": (
                    "Inventory turns measure how many times stock is sold and replaced in a year. Higher turns mean "
                    "less capital tied up in inventory. At 8.6x with 28.4 days of supply, MakerIQ balances just-in-time "
                    "efficiency against stockout risk using ML-based demand forecasting. The platform classifies every "
                    "component as HEALTHY, LOW, or CRITICAL based on predicted days of stock and forecast demand."
                ),
            },
        ],
        "page_details": [
            {"name": "Plant Performance Hub", "desc": "Executive command center with real-time OEE, production output, quality metrics, and AI morning briefing. Covers all 5 global facilities (Austin, Detroit, Shanghai, Stuttgart, Guadalajara) with drill-down to individual production lines."},
            {"name": "Production Analytics", "desc": "Throughput tracking at 1,240 units/hour, cycle time analysis (42.6 sec average), changeover optimization, and shift-level performance comparisons. Identifies bottleneck machines and scheduling optimization opportunities."},
            {"name": "Quality Control", "desc": "SPC charts, tolerance monitoring, FPY tracking, and root-cause analysis with SHAP-powered defect attribution. Cpk analysis by process step with real-time alerts when capability indices drop below threshold."},
            {"name": "Supply Chain & Inventory", "desc": "Demand forecasting with 90-day horizon, stock level monitoring with HEALTHY/LOW/CRITICAL classification, supplier lead time tracking, and purchase order optimization. Prevents both stockouts and excess inventory."},
            {"name": "Predictive Maintenance", "desc": "ML-scored anomaly detection on CNC machine telemetry (vibration, temperature, tool wear). SHAP explainability for every flag, MTTR tracking at 2.4 hours, and maintenance schedule optimization. The heart of MakerIQ's value proposition."},
            {"name": "Energy & Sustainability", "desc": "Energy consumption by facility and line, carbon footprint tracking, utility cost optimization, and sustainability KPI monitoring. Connects energy usage to production output for efficiency benchmarking."},
            {"name": "Workforce Operations", "desc": "Headcount tracking (2,840 across 5 plants), overtime analysis (12% rate), safety incident monitoring (1.2 rate), and skills matrix management. Workforce scheduling optimization based on production demand forecasts."},
        ],
        "genie_section": (
            "MakerIQ's Genie AI is tuned for manufacturing operations. Ask 'Which machines had anomaly scores above "
            "0.85 in the last 24 hours?' and get a prioritized maintenance list. Ask 'What is our projected stockout "
            "risk for EV Drive Units at the Austin plant?' and get a forecast with confidence intervals. The assistant "
            "understands OEE decomposition, SPC terminology, and supply chain concepts natively across 285+ pre-trained questions."
        ),
        "business_impact": [
            {"metric": "24-72 hrs", "label": "advance failure prediction window"},
            {"metric": "90 days", "label": "demand forecast horizon per component"},
            {"metric": "5 plants", "label": "monitored from one unified platform"},
            {"metric": "40%", "label": "reduction in unplanned downtime with ML"},
        ],
    },
}
# Merge remaining verticals
import importlib.util
_spec = importlib.util.spec_from_file_location("_v2", Path(__file__).parent / "_verticals2.py")
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
VERTICALS.update(_mod.VERTICALS_2)


def load_config(vertical_key):
    yaml_path = CONFIG_DIR / f"{vertical_key}.yaml"
    if not yaml_path.exists():
        return {}
    with open(yaml_path) as f:
        return yaml.safe_load(f)


def get_pages(cfg):
    return [p for p in cfg.get("pages", []) if p.get("id") not in ("architecture", "details")]


def get_ml_models(cfg):
    ml = cfg.get("ml", {})
    models = []
    for k, v in ml.items():
        if isinstance(v, dict) and "name" in v:
            models.append(v)
    return models


def get_genie_tables(cfg):
    return cfg.get("genie", {}).get("tables", [])


def get_genie_question_count(cfg):
    questions = cfg.get("genie", {}).get("sample_questions", [])
    if not questions:
        return 0
    if isinstance(questions[0], str):
        return len(questions)
    count = 0
    for cat in questions:
        if isinstance(cat, dict):
            count += len(cat.get("questions", []))
    return count


def sanitize(s):
    out = []
    for ch in s:
        cp = ord(ch)
        if 0xD800 <= cp <= 0xDFFF:
            out.append(f"&#x{cp:X};")
        else:
            out.append(ch)
    return "".join(out)


def build_page(vkey, info):
    cfg = load_config(vkey)
    app_cfg = cfg.get("app", {})
    pages = get_pages(cfg)
    models = get_ml_models(cfg)
    tables = get_genie_tables(cfg)
    q_count = get_genie_question_count(cfg)
    catalog = app_cfg.get("catalog", "")
    accent = info["accent"]
    product = info["product_name"]
    page_details = info.get("page_details", [])

    # ── KPI section ──
    kpis_html = ""
    for m in info["metric_deep_dives"]:
        kpis_html += f'''
            <div class="text-center p-5 bg-white border border-gray-100 hover:shadow-sm transition-shadow">
              <p class="text-2xl font-bold tracking-tight" style="color: {accent}">{m["value"]}</p>
              <p class="text-xs font-bold text-gray-900 uppercase tracking-wider mt-1">{m["name"]}</p>
              <p class="text-[10px] text-gray-400 mt-0.5">{m["context"]}</p>
            </div>'''

    # ── Metric deep dives ──
    metrics_html = ""
    for m in info["metric_deep_dives"]:
        metrics_html += f'''
              <div class="p-6 bg-gray-50 border border-gray-100">
                <div class="flex items-baseline gap-3 mb-3">
                  <span class="text-xl font-bold" style="color: {accent}">{m["value"]}</span>
                  <span class="text-sm font-bold text-gray-900">{m["name"]}</span>
                  <span class="text-[10px] text-gray-400 ml-auto">{m["context"]}</span>
                </div>
                <p class="text-sm text-gray-600 leading-relaxed">{m["explanation"]}</p>
              </div>'''

    # ── Pages with rich descriptions ──
    pages_detail_html = ""
    for i, pd in enumerate(page_details):
        num = str(i + 1).zfill(2)
        pages_detail_html += f'''
              <div class="p-6 bg-gray-50 border border-gray-100 hover:border-gray-300 transition-colors">
                <div class="flex items-center gap-3 mb-3">
                  <span class="text-[10px] font-bold uppercase tracking-widest px-2 py-1 text-white" style="background:{accent}">{num}</span>
                  <h4 class="font-bold text-gray-900">{pd["name"]}</h4>
                </div>
                <p class="text-sm text-gray-600 leading-relaxed">{pd["desc"]}</p>
              </div>'''

    # ── ML models ──
    models_html = ""
    for m in models:
        name = m.get("name", "").replace("_", " ")
        algo = m.get("algorithm", "")
        tm = m.get("target_metric", "")
        tv = m.get("target_value", "")
        feats = m.get("features", [])
        feat_str = ", ".join(feats[:6])
        if len(feats) > 6:
            feat_str += f" +{len(feats)-6} more"
        models_html += f'''
              <div class="bg-white border border-gray-200 p-6 hover:shadow-md transition-shadow">
                <div class="flex items-center justify-between mb-3">
                  <h4 class="font-bold text-gray-900">{name}</h4>
                  <span class="text-[10px] font-bold uppercase tracking-widest px-2 py-1 border" style="color:{accent}; border-color:{accent}">{algo}</span>
                </div>
                <div class="grid grid-cols-2 gap-4 text-xs text-gray-600 mt-3">
                  <div>
                    <span class="font-bold text-gray-500 uppercase tracking-wider text-[10px]">Target Metric</span>
                    <p class="mt-1 font-bold text-gray-900">{tm} = {tv}</p>
                  </div>
                  <div>
                    <span class="font-bold text-gray-500 uppercase tracking-wider text-[10px]">Features</span>
                    <p class="mt-1">{feat_str}</p>
                  </div>
                </div>
              </div>'''

    # ── Business Impact ──
    impact_html = ""
    for b in info.get("business_impact", []):
        impact_html += f'''
              <div class="text-center p-6 bg-white border border-gray-100">
                <p class="text-2xl font-bold" style="color: {accent}">{b["metric"]}</p>
                <p class="text-xs text-gray-600 mt-1">{b["label"]}</p>
              </div>'''

    # ── Tables ──
    tables_html = ""
    for t in tables:
        tables_html += f'''
                  <div class="flex items-center gap-2 text-xs text-gray-600">
                    <svg class="w-3 h-3 flex-shrink-0" style="color:{accent}" fill="currentColor" viewBox="0 0 20 20"><path fill-rule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clip-rule="evenodd"/></svg>
                    <span class="font-mono">{t}</span>
                  </div>'''

    # ── Use cases from metric names ──
    use_cases = [pd["name"] for pd in page_details[:6]]
    uc_html = ""
    for uc in use_cases:
        uc_html += f'<span class="px-3 py-1.5 bg-gray-100 text-xs text-gray-700 border border-gray-200">{uc}</span>'

    page_count = len(pages)
    model_count = len(models)

    return f'''<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{product} | Blueprint IQ</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700&display=swap" rel="stylesheet">
  <script>
    tailwind.config = {{
      theme: {{
        extend: {{
          colors: {{ 'bp-accent': '{accent}' }},
          fontFamily: {{ sans: ['DM Sans', 'system-ui', 'sans-serif'] }},
        }}
      }}
    }}
  </script>
  <style>
    body {{ font-family: 'DM Sans', system-ui, sans-serif; }}
    .sharp-card {{ border: 1px solid #e5e7eb; background: #fff; }}
    .sharp-card:hover {{ box-shadow: 0 4px 12px rgba(0,0,0,0.08); }}
    .arch-layer {{ transition: transform 0.2s; }}
    .arch-layer:hover {{ transform: translateY(-2px); }}
  </style>
</head>
<body class="bg-[#f8f9fa] min-h-screen">
  <!-- Top Bar -->
  <div class="bg-white border-b border-gray-200 sticky top-0 z-50">
    <div class="container mx-auto px-4 sm:px-6 lg:px-8 flex items-center justify-between h-14">
      <div class="flex items-center gap-3">
        <span class="text-xs font-bold uppercase tracking-[0.2em] text-gray-400">Blueprint</span>
        <span class="text-gray-300">|</span>
        <span class="text-xs font-bold uppercase tracking-widest" style="color: {accent}">{product}</span>
      </div>
      <a href="../index.html" class="text-xs font-bold uppercase tracking-widest text-gray-400 hover:text-gray-900 transition-colors">&larr; All Verticals</a>
    </div>
  </div>

  <main class="container mx-auto px-4 sm:px-6 lg:px-8 py-10">
    <!-- Breadcrumb -->
    <nav aria-label="Breadcrumb" class="mb-6 text-sm text-gray-400">
      <ol class="flex items-center gap-1.5">
        <li><a href="../index.html" class="hover:text-gray-900 transition-colors">Blueprint IQ</a></li>
        <li><span class="mx-1">/</span></li>
        <li class="text-gray-900 font-medium">{product}</li>
      </ol>
    </nav>

    <!-- Hero -->
    <header class="mb-10">
      <div class="flex items-center gap-4 mb-3">
        <span class="text-4xl">{info["icon"]}</span>
        <div>
          <h1 class="text-3xl md:text-4xl font-bold text-gray-900 tracking-tight">{product}</h1>
          <p class="text-lg font-medium mt-1" style="color: {accent}">{info["hero_tagline"]}</p>
        </div>
      </div>
      <p class="text-base text-gray-600 leading-relaxed max-w-3xl mt-4">{info["hero_description"]}</p>
    </header>

    <!-- North Star KPIs -->
    <section class="mb-10">
      <div class="flex items-center gap-3 mb-4">
        <h2 class="text-[10px] font-bold uppercase tracking-[0.2em] text-gray-400">North Star Metrics</h2>
        <div class="flex-1 h-px bg-gray-200"></div>
      </div>
      <div class="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">{kpis_html}
      </div>
    </section>

    <div class="grid grid-cols-1 lg:grid-cols-10 gap-12">
      <!-- ══ Main Content (7 cols) ══ -->
      <div class="lg:col-span-7 space-y-10">

        <!-- The Challenge -->
        <section class="bg-white p-8 border border-gray-200">
          <h2 class="text-xl font-bold text-gray-900 border-b border-gray-100 pb-3 mb-5">The Challenge</h2>
          <p class="text-sm text-gray-600 leading-relaxed">{info["the_challenge"]}</p>
        </section>

        <!-- The Solution -->
        <section class="bg-white p-8 border border-gray-200">
          <h2 class="text-xl font-bold text-gray-900 border-b border-gray-100 pb-3 mb-5">The Solution</h2>
          <p class="text-sm text-gray-600 leading-relaxed">{info["the_solution"]}</p>
        </section>

        <!-- Screenshot placeholder -->
        <section class="sharp-card p-1 shadow-sm bg-[#0D1117]">
          <div class="aspect-[16/7] w-full flex items-center justify-center">
            <div class="text-center px-6">
              <span class="text-6xl block mb-3">{info["icon"]}</span>
              <p class="text-gray-400 font-bold text-sm">{product} Dashboard</p>
              <p class="text-gray-500 text-[10px] uppercase tracking-widest mt-2">Databricks App &middot; Dark Theme &middot; 7 Purpose-Built Views</p>
            </div>
          </div>
        </section>

        <!-- Why These Metrics Matter -->
        <section class="bg-white p-8 border border-gray-200">
          <h2 class="text-xl font-bold text-gray-900 border-b border-gray-100 pb-3 mb-5">Why These Metrics Matter</h2>
          <p class="text-sm text-gray-500 mb-6">Every metric tracked in {product} is tied to a business outcome. Here is why each one matters and how the platform measures it.</p>
          <div class="space-y-4">{metrics_html}
          </div>
        </section>

        <!-- Application Pages -->
        <section class="bg-white p-8 border border-gray-200">
          <div class="flex justify-between items-end mb-6 border-b border-gray-100 pb-4">
            <h2 class="text-xl font-bold text-gray-900">What You Get: {page_count} Application Views</h2>
          </div>
          <div class="space-y-4">{pages_detail_html}
          </div>
        </section>

        <!-- ML & AI Models -->
        <section class="bg-white p-8 border border-gray-200">
          <div class="flex justify-between items-end mb-6 border-b border-gray-100 pb-4">
            <h2 class="text-xl font-bold text-gray-900">ML Models in Production</h2>
            <span class="text-xs text-gray-400 font-medium">{model_count} models &middot; MLflow Registry</span>
          </div>
          <div class="space-y-4">{models_html}
          </div>
        </section>

        <!-- Genie AI -->
        <section class="bg-white p-8 border border-gray-200">
          <div class="flex items-center gap-3 mb-5 border-b border-gray-100 pb-3">
            <span class="text-2xl">&#x1F9DE;</span>
            <h2 class="text-xl font-bold text-gray-900">Genie AI Assistant</h2>
          </div>
          <p class="text-sm text-gray-600 leading-relaxed">{info["genie_section"]}</p>
        </section>

        <!-- Architecture -->
        <section class="bg-white p-8 border border-gray-200">
          <h2 class="text-xl font-bold text-gray-900 border-b border-gray-100 pb-3 mb-6">Lakehouse Architecture</h2>
          <p class="text-sm text-gray-500 mb-6">
            {product} is built on the Databricks Medallion Architecture &mdash; a three-layer data pipeline that
            transforms raw source data into business-ready analytics with built-in quality, governance, and ML scoring at every stage.
          </p>
          <div class="space-y-3">
            <div class="arch-layer flex items-stretch">
              <div class="w-28 flex-shrink-0 flex items-center justify-center text-white text-[10px] font-bold uppercase tracking-widest p-3" style="background:#92400e">Bronze</div>
              <div class="flex-1 bg-amber-50 border border-amber-200 p-4">
                <p class="text-xs font-bold text-amber-900 mb-1">Raw Ingestion Layer</p>
                <p class="text-[11px] text-amber-700">Auto Loader streaming and batch ingestion from source systems &mdash; transactional databases, IoT sensors, ERP feeds, and external APIs. Schema-on-read with audit columns and lineage tracking. Data arrives in its original form, immutable and queryable.</p>
              </div>
            </div>
            <div class="arch-layer flex items-stretch">
              <div class="w-28 flex-shrink-0 flex items-center justify-center text-white text-[10px] font-bold uppercase tracking-widest p-3" style="background:#6b7280">Silver</div>
              <div class="flex-1 bg-gray-50 border border-gray-200 p-4">
                <p class="text-xs font-bold text-gray-900 mb-1">Curated &amp; Enriched</p>
                <p class="text-[11px] text-gray-600">Data quality checks, deduplication, business rule application, cross-source joins, and ML scoring. This is where models run &mdash; anomaly detection, churn scoring, and feature engineering happen in Silver, creating enriched records ready for analytics.</p>
              </div>
            </div>
            <div class="arch-layer flex items-stretch">
              <div class="w-28 flex-shrink-0 flex items-center justify-center text-white text-[10px] font-bold uppercase tracking-widest p-3" style="background:{accent}">Gold</div>
              <div class="flex-1 border p-4" style="background:{accent}08; border-color:{accent}30">
                <p class="text-xs font-bold text-gray-900 mb-1">Business-Ready Analytics</p>
                <p class="text-[11px] text-gray-600">KPI aggregates, time-series snapshots, forecast outputs, and model health metrics &mdash; all optimized for dashboard queries. Gold tables power every chart, metric, and insight card in the {product} interface.</p>
              </div>
            </div>
          </div>
          <div class="mt-6 grid grid-cols-1 md:grid-cols-3 gap-3">
            <div class="p-4 bg-gray-50 border border-gray-100 text-center">
              <p class="text-xs font-bold text-gray-500 uppercase tracking-wider">Unity Catalog</p>
              <p class="text-sm font-bold text-gray-900 mt-1">{catalog}</p>
            </div>
            <div class="p-4 bg-gray-50 border border-gray-100 text-center">
              <p class="text-xs font-bold text-gray-500 uppercase tracking-wider">MLflow Registry</p>
              <p class="text-sm font-bold text-gray-900 mt-1">{model_count} Models</p>
            </div>
            <div class="p-4 bg-gray-50 border border-gray-100 text-center">
              <p class="text-xs font-bold text-gray-500 uppercase tracking-wider">Genie AI</p>
              <p class="text-sm font-bold text-gray-900 mt-1">{q_count}+ Questions</p>
            </div>
          </div>
        </section>

        <!-- Business Impact -->
        <section class="bg-white p-8 border border-gray-200">
          <h2 class="text-xl font-bold text-gray-900 border-b border-gray-100 pb-3 mb-6">Business Impact</h2>
          <div class="grid grid-cols-2 md:grid-cols-4 gap-4">{impact_html}
          </div>
        </section>

      </div>

      <!-- ══ Sidebar (3 cols) ══ -->
      <aside class="lg:col-span-3">
        <div class="sticky top-20 space-y-6">
          <div class="sharp-card p-6">
            <h3 class="text-[10px] font-bold uppercase tracking-[0.2em] mb-5" style="color:{accent}">Quick Facts</h3>
            <div class="space-y-4 text-sm">
              <div class="flex justify-between pb-3 border-b border-gray-100"><span class="text-gray-500">Application Pages</span><span class="font-bold text-gray-900">{page_count}</span></div>
              <div class="flex justify-between pb-3 border-b border-gray-100"><span class="text-gray-500">ML Models</span><span class="font-bold text-gray-900">{model_count}</span></div>
              <div class="flex justify-between pb-3 border-b border-gray-100"><span class="text-gray-500">Genie Questions</span><span class="font-bold text-gray-900">{q_count}+</span></div>
              <div class="flex justify-between pb-3 border-b border-gray-100"><span class="text-gray-500">Unity Catalog</span><span class="font-bold text-gray-900 font-mono text-xs">{catalog}</span></div>
              <div class="flex justify-between"><span class="text-gray-500">Platform</span><span class="font-bold text-gray-900">Databricks</span></div>
            </div>
          </div>

          <div class="bg-white p-6 border border-gray-200">
            <h3 class="text-[10px] font-bold uppercase tracking-[0.2em] text-gray-400 mb-4">Core Capabilities</h3>
            <div class="flex flex-wrap gap-2">{uc_html}</div>
          </div>

          {"" if not tables else f"""<div class="bg-white p-6 border border-gray-200">
            <h3 class="text-[10px] font-bold uppercase tracking-[0.2em] text-gray-400 mb-4">Lakehouse Tables</h3>
            <div class="space-y-2">{tables_html}</div>
          </div>"""}

          <div class="sharp-card p-6 border-t-4" style="border-top-color:{accent}">
            <h3 class="text-[10px] font-bold uppercase tracking-[0.2em] text-gray-400 mb-4">Technology Stack</h3>
            <div class="space-y-3">
              {"".join(f'<div class="flex items-center gap-3"><div class="w-2 h-2 flex-shrink-0" style="background:{accent}"></div><span class="text-xs text-gray-700">{t}</span></div>' for t in ["Databricks Lakehouse", "Unity Catalog", "MLflow Model Registry", "Genie AI (Claude Sonnet)", "Dash / Plotly", "Delta Lake", "SHAP Explainability"])}
            </div>
          </div>

          <div class="bg-white p-6 border border-gray-200">
            <h3 class="text-[10px] font-bold uppercase tracking-[0.2em] text-gray-400 mb-4">Resources</h3>
            <ul class="space-y-3">
              <li><span class="text-sm font-bold cursor-pointer" style="color:{accent}">View Technical Architecture</span></li>
              <li><span class="text-sm font-bold cursor-pointer" style="color:{accent}">Download Sample Data</span></li>
              <li><span class="text-sm font-bold cursor-pointer" style="color:{accent}">Case Studies</span></li>
            </ul>
          </div>
        </div>
      </aside>
    </div>
  </main>

  <footer class="border-t border-gray-200 mt-20 py-8">
    <div class="container mx-auto px-4 sm:px-6 lg:px-8 text-center">
      <p class="text-[10px] font-bold uppercase tracking-[0.2em] text-gray-400">Powered by Databricks &middot; Blueprint AI Platform</p>
    </div>
  </footer>
</body>
</html>'''


def build_index():
    cards = ""
    for key, info in VERTICALS.items():
        ms = info["metric_deep_dives"][:3]
        kpi_chips = "".join(
            f'<span class="text-[10px] text-gray-500">{m["name"]}: <strong style="color:{info["accent"]}">{m["value"]}</strong></span> '
            for m in ms
        )
        desc = info["hero_description"][:180]
        cards += f'''
        <a href="verticals/{info["slug"]}.html" class="sharp-card p-6 hover:shadow-lg transition-all group block">
          <div class="flex items-center gap-3 mb-3">
            <span class="text-3xl">{info["icon"]}</span>
            <div>
              <h2 class="text-lg font-bold text-gray-900 group-hover:text-blue-600 transition-colors">{info["product_name"]}</h2>
              <p class="text-xs text-gray-400">{info["hero_tagline"]}</p>
            </div>
          </div>
          <p class="text-sm text-gray-600 leading-relaxed mb-4 line-clamp-2">{desc}...</p>
          <div class="flex flex-wrap gap-3 mb-4">{kpi_chips}</div>
          <span class="text-[10px] font-bold uppercase tracking-widest" style="color:{info["accent"]}">View Details &rarr;</span>
        </a>'''

    return f'''<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Blueprint IQ | Industry Verticals</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700&display=swap" rel="stylesheet">
  <script>
    tailwind.config = {{ theme: {{ extend: {{ colors: {{ 'bp-blue': '#1d4ed8' }}, fontFamily: {{ sans: ['DM Sans', 'system-ui', 'sans-serif'] }} }} }} }}
  </script>
  <style>body {{ font-family: 'DM Sans', system-ui, sans-serif; }} .sharp-card {{ border: 1px solid #e5e7eb; background: #fff; }}</style>
</head>
<body class="bg-[#f8f9fa] min-h-screen">
  <div class="bg-white border-b border-gray-200 sticky top-0 z-50">
    <div class="container mx-auto px-4 sm:px-6 lg:px-8 flex items-center h-14">
      <span class="text-xs font-bold uppercase tracking-[0.2em] text-gray-400">Blueprint</span>
      <span class="text-gray-300 mx-3">|</span>
      <span class="text-xs font-bold uppercase tracking-widest text-bp-blue">Industry Verticals</span>
    </div>
  </div>
  <main class="container mx-auto px-4 sm:px-6 lg:px-8 py-10">
    <header class="mb-10">
      <h1 class="text-3xl md:text-4xl font-bold text-gray-900 tracking-tight mb-2">Blueprint IQ Platform</h1>
      <p class="text-lg text-bp-blue font-medium">Industry-specific intelligence apps built on Databricks Lakehouse</p>
      <p class="text-sm text-gray-500 mt-3 max-w-3xl">
        Each vertical delivers a production-ready analytics application with ML models, AI-powered Genie assistant,
        and a medallion architecture data pipeline &mdash; purpose-built for domain-specific KPIs and workflows.
      </p>
    </header>
    <div class="flex items-center gap-3 mb-6">
      <h2 class="text-[10px] font-bold uppercase tracking-[0.2em] text-gray-400">{len(VERTICALS)} Verticals</h2>
      <div class="flex-1 h-px bg-gray-200"></div>
    </div>
    <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">{cards}
    </div>
  </main>
  <footer class="border-t border-gray-200 mt-20 py-8">
    <div class="container mx-auto px-4 sm:px-6 lg:px-8 text-center">
      <p class="text-[10px] font-bold uppercase tracking-[0.2em] text-gray-400">Powered by Databricks &middot; Blueprint AI Platform</p>
    </div>
  </footer>
</body>
</html>'''


def main():
    index_path = ROOT / "docs" / "index.html"
    index_path.write_text(sanitize(build_index()), encoding="utf-8")
    print("  index.html")

    for key, info in VERTICALS.items():
        html = sanitize(build_page(key, info))
        out_path = OUT_DIR / f"{info['slug']}.html"
        out_path.write_text(html, encoding="utf-8")
        print(f"  verticals/{info['slug']}.html")

    print(f"\nDone! Generated {len(VERTICALS) + 1} pages in docs/")


if __name__ == "__main__":
    main()
