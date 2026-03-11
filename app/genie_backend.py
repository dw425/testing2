"""
Genie AI chat backend for Blueprint IQ (v2).

Always tries the Foundation Model first (via databricks-claude-sonnet-4-6),
building a rich data context from YAML config for each vertical.
Falls back to keyword-matched demo responses if the FM call fails.

Public API:
    ask_genie(question, use_case) -> dict
"""

import os
import re
import time
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# ===================================================================
#  Data summary cache
# ===================================================================

_DATA_SUMMARY_CACHE: Dict[str, Dict[str, Any]] = {}
# Structure: { use_case: {"summary": str, "timestamp": float} }

_CACHE_TTL_SECONDS = 300  # 5 minutes


def _is_cache_valid(use_case: str) -> bool:
    """Check whether the cached summary for a vertical is still fresh."""
    entry = _DATA_SUMMARY_CACHE.get(use_case)
    if entry is None:
        return False
    return (time.time() - entry["timestamp"]) < _CACHE_TTL_SECONDS


# ===================================================================
#  Helper utilities
# ===================================================================

def _safe_call(fn, *args, **kwargs):
    """Call a data function, returning an empty list/dict on failure."""
    try:
        result = fn(*args, **kwargs)
        return result if result is not None else []
    except Exception as e:
        logger.debug("Data function %s failed: %s", fn.__name__, e)
        return []


def _summarize_list(data: list, label: str, max_rows: int = 5) -> str:
    """Produce a compact text table from a list of dicts."""
    if not data:
        return f"{label}: No data available.\n"
    keys = list(data[0].keys())
    header = " | ".join(keys)
    lines = [f"{label} ({len(data)} rows, showing top {min(max_rows, len(data))}):", header]
    for row in data[:max_rows]:
        lines.append(" | ".join(str(row.get(k, "")) for k in keys))
    return "\n".join(lines) + "\n"


def _agg_by(data: list, group_key: str, value_key: str) -> str:
    """Group data by a key and compute avg/count for a value."""
    if not data:
        return ""
    groups: Dict[str, List[float]] = {}
    for row in data:
        g = str(row.get(group_key, "unknown"))
        val = row.get(value_key)
        if val is not None:
            try:
                groups.setdefault(g, []).append(float(val))
            except (ValueError, TypeError):
                pass
    if not groups:
        return ""
    lines = [f"  Breakdown by {group_key} (avg {value_key}):"]
    for g, vals in sorted(groups.items(), key=lambda x: -sum(x[1]) / len(x[1]) if x[1] else 0):
        avg = sum(vals) / len(vals)
        lines.append(f"    {g}: avg={avg:.2f}, count={len(vals)}")
    return "\n".join(lines) + "\n"


# ===================================================================
#  Config-based data summary builder
# ===================================================================

def _build_config_summary(use_case: str) -> str:
    """Build a data summary from the vertical's YAML config."""
    from app.data_access import get_config_for
    cfg = get_config_for(use_case)

    parts = []
    app_cfg = cfg.get("app", {})
    parts.append(f"=== {app_cfg.get('title', use_case)} ===")
    parts.append(f"Subtitle: {app_cfg.get('subtitle', '')}")
    parts.append(f"Catalog: {app_cfg.get('catalog', '')}")
    parts.append("")

    # Data section - dump all metrics
    data_cfg = cfg.get("data", {})
    for key, value in data_cfg.items():
        if isinstance(value, dict):
            parts.append(f"=== {key.replace('_', ' ').title()} ===")
            for k, v in value.items():
                parts.append(f"  {k}: {v}")
            parts.append("")
        elif isinstance(value, list) and value and isinstance(value[0], str):
            parts.append(f"  {key}: {', '.join(value)}")

    # ML models
    ml_cfg = cfg.get("ml", {})
    for model_key, model in ml_cfg.items():
        if isinstance(model, dict) and "name" in model:
            parts.append(f"\n=== ML Model: {model['name']} ===")
            parts.append(f"  Algorithm: {model.get('algorithm', 'N/A')}")
            parts.append(f"  Target: {model.get('target_metric', 'N/A')} = {model.get('target_value', 'N/A')}")
            if 'features' in model:
                parts.append(f"  Features: {', '.join(model['features'])}")

    # Dashboard KPIs
    kpis = cfg.get("dashboard", {}).get("kpis", [])
    if kpis:
        parts.append("\n=== Dashboard KPIs ===")
        for kpi in kpis:
            parts.append(f"  {kpi['title']}: {kpi.get('value', 'N/A')}")

    # Genie tables
    tables = cfg.get("genie", {}).get("tables", [])
    if tables:
        parts.append(f"\n=== Lakehouse Tables ===")
        for t in tables:
            parts.append(f"  {t}")

    return "\n".join(parts)


# ===================================================================
#  Summary builders map
# ===================================================================

_SUMMARY_BUILDERS = {
    "gaming": lambda: _build_config_summary("gaming"),
    "telecom": lambda: _build_config_summary("telecom"),
    "media": lambda: _build_config_summary("media"),
    "financial_services": lambda: _build_config_summary("financial_services"),
    "hls": lambda: _build_config_summary("hls"),
    "manufacturing": lambda: _build_config_summary("manufacturing"),
    "risk": lambda: _build_config_summary("risk"),
}


def _get_data_summary(use_case: str) -> str:
    """Return a cached or freshly built data summary for the vertical."""
    if _is_cache_valid(use_case):
        return _DATA_SUMMARY_CACHE[use_case]["summary"]

    builder = _SUMMARY_BUILDERS.get(use_case)
    if builder is None:
        summary = f"No detailed data available for vertical: {use_case}"
    else:
        try:
            summary = builder()
        except Exception as e:
            logger.warning("Failed to build data summary for %s: %s", use_case, e)
            summary = f"Data summary unavailable for {use_case}: {e}"

    _DATA_SUMMARY_CACHE[use_case] = {"summary": summary, "timestamp": time.time()}
    return summary


# ===================================================================
#  System prompt builder
# ===================================================================

_VERTICAL_DESCRIPTIONS = {
    "gaming": "player analytics, game development, and live game operations",
    "telecom": "network operations, subscriber management, and telecommunications",
    "media": "audience intelligence, content analytics, and media monetization",
    "financial_services": "banking, capital markets, and insurance analytics",
    "hls": "healthcare providers, health plans, biopharma, and medtech analytics",
    "manufacturing": "production analytics, quality control, predictive maintenance, and supply chain",
    "risk": "enterprise risk management, credit risk, market risk, operational risk, and compliance",
}

_APP_NAMES = {
    "gaming": "Gaming IQ",
    "telecom": "Telecom IQ",
    "media": "Media IQ",
    "financial_services": "Financial Services IQ",
    "hls": "Health & Life Sciences IQ",
    "manufacturing": "Manufacturing IQ",
    "risk": "Risk IQ",
}


def _build_fm_system_prompt(use_case: str) -> str:
    """Build a rich system prompt with live data context for the FM."""
    app_name = _APP_NAMES.get(use_case, "Blueprint IQ")
    vertical_desc = _VERTICAL_DESCRIPTIONS.get(use_case, "data analytics")
    data_summary = _get_data_summary(use_case)

    return (
        f"You are a data analyst for {app_name}, a {vertical_desc} analytics platform.\n\n"
        f"You have access to the following live data:\n\n"
        f"{data_summary}\n\n"
        f"Answer questions concisely and accurately based on this data. Include specific numbers.\n"
        f"When relevant, mention trends, comparisons, and actionable insights.\n"
        f"If you generate SQL, wrap it in ```sql blocks.\n"
        f"Format your response with bold headers using **text** for key metrics.\n"
        f"Keep responses focused and under 200 words."
    )


# ===================================================================
#  FM call
# ===================================================================

def _ask_fm(question: str, use_case: str) -> Dict[str, Any]:
    """Call the databricks-claude-sonnet-4-6 Foundation Model endpoint."""
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    system_prompt = _build_fm_system_prompt(use_case)

    response = w.serving_endpoints.query(
        name="databricks-claude-sonnet-4-6",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": question},
        ],
        max_tokens=1024,
        temperature=0.2,
    )

    answer_text = ""
    if hasattr(response, "choices") and response.choices:
        choice = response.choices[0]
        if hasattr(choice, "message") and hasattr(choice.message, "content"):
            answer_text = choice.message.content

    # Extract SQL if present in the response
    sql_text = None
    sql_match = re.search(r"```sql\s*(.*?)```", answer_text, re.DOTALL)
    if sql_match:
        sql_text = sql_match.group(1).strip()

    return {
        "answer": answer_text.strip() or "The model returned an empty response.",
        "sql": sql_text,
        "source": "fm",
        "data": None,
    }


# ===================================================================
#  Demo mode -- intelligent keyword-matched responses (fallback)
# ===================================================================

_DEMO_RESPONSES: Dict[str, List[Dict[str, Any]]] = {
    # -----------------------------------------------------------------
    # GAMING
    # -----------------------------------------------------------------
    "gaming": [
        {
            "patterns": [r"retention", r"churn", r"player", r"d1", r"d7", r"d30"],
            "answer": (
                "**Player Retention & Churn Analysis**\n\n"
                "Current retention rates across all titles:\n"
                "- **D1 Retention:** 68% (industry avg: 40%)\n"
                "- **D7 Retention:** 41% (industry avg: 20%)\n"
                "- **D30 Retention:** 22% (industry avg: 10%)\n\n"
                "**Churn Risk by Segment:**\n"
                "The Player_Churn_Predictor model (AUC-ROC: 0.91) flags **9.2%** of Dolphin-segment "
                "players as high churn risk -- these are mid-spending players with no purchase in 14+ days. "
                "Whales show 7% high churn risk with D7 retention at 64%.\n\n"
                "**Cohort Breakdown:**\n"
                "| Segment | D1 | D7 | D30 | Avg LTV |\n"
                "|---------|-----|-----|------|--------|\n"
                "| Whale | 82% | 64% | 45% | $284.50 |\n"
                "| Dolphin | 74% | 48% | 28% | $68.20 |\n"
                "| Minnow | 65% | 35% | 15% | $12.40 |\n"
                "| Free-to-Play | 58% | 28% | 8% | $2.10 |\n\n"
                "**Recommendation:** Target Dolphin-segment churners with a 20% discount offer to "
                "recover an estimated $560K in at-risk LTV."
            ),
            "sql": (
                "SELECT segment, d1_retention, d7_retention, d30_retention, avg_ltv,\n"
                "       churn_risk_pct\n"
                "FROM gaming_iq.gold.player_retention_cohorts\n"
                "ORDER BY avg_ltv DESC"
            ),
            "data": [
                {"segment": "Whale", "d1": "82%", "d7": "64%", "d30": "45%", "ltv": "$284.50", "churn_risk": "7%"},
                {"segment": "Dolphin", "d1": "74%", "d7": "48%", "d30": "28%", "ltv": "$68.20", "churn_risk": "9.2%"},
                {"segment": "Minnow", "d1": "65%", "d7": "35%", "d30": "15%", "ltv": "$12.40", "churn_risk": "4.1%"},
                {"segment": "Free-to-Play", "d1": "58%", "d7": "28%", "d30": "8%", "ltv": "$2.10", "churn_risk": "1.7%"},
            ],
        },
        {
            "patterns": [r"revenue", r"arpdau", r"monetiz", r"iap", r"ad"],
            "answer": (
                "**Revenue & Monetization Overview**\n\n"
                "Overall ARPDAU: **$0.118** | Daily Revenue: **$283K**\n\n"
                "**ARPDAU by Region:**\n"
                "| Region | ARPDAU | DAU | Daily Revenue | Top Driver |\n"
                "|--------|--------|-----|-------------|------------|\n"
                "| APAC-JP | $0.182 | 380K | $69.2K | Gacha mechanics |\n"
                "| NA-East | $0.142 | 620K | $88.0K | Battle pass |\n"
                "| EU-West | $0.104 | 540K | $56.2K | Cosmetics |\n"
                "| NA-West | $0.098 | 420K | $41.2K | Battle pass |\n\n"
                "**Revenue Mix:**\n"
                "- In-App Purchases (IAP): 62% of revenue\n"
                "- Battle Pass subscriptions: 24% of revenue\n"
                "- Ad monetization: 14% of revenue\n\n"
                "APAC-JP leads ARPDAU at $0.182, driven by Stellar Conquest's gacha-style limited-time "
                "event shop. NA-East generates the highest absolute revenue due to larger player base.\n\n"
                "**Recommendation:** Expand gacha-style events to EU-West where cosmetics dominate -- "
                "estimated uplift of $8-12K/day."
            ),
            "sql": (
                "SELECT region, arpdau, dau, daily_revenue,\n"
                "       top_revenue_driver\n"
                "FROM gaming_iq.gold.economy_health_metrics\n"
                "ORDER BY arpdau DESC"
            ),
            "data": [
                {"region": "APAC-JP", "arpdau": "$0.182", "dau": "380K", "daily_revenue": "$69.2K"},
                {"region": "NA-East", "arpdau": "$0.142", "dau": "620K", "daily_revenue": "$88.0K"},
                {"region": "EU-West", "arpdau": "$0.104", "dau": "540K", "daily_revenue": "$56.2K"},
                {"region": "NA-West", "arpdau": "$0.098", "dau": "420K", "daily_revenue": "$41.2K"},
            ],
        },
        {
            "patterns": [r"server", r"uptime", r"mttr", r"latency", r"infrastructure"],
            "answer": (
                "**Server Operations & Infrastructure Health**\n\n"
                "Overall server uptime: **99.94%** | Avg latency: **28ms**\n"
                "Total active servers: **2,400** across 6 regions\n\n"
                "**Regional Performance:**\n"
                "| Region | Uptime | Latency | Servers | Incidents (7d) |\n"
                "|--------|--------|---------|---------|----------------|\n"
                "| NA-East | 99.99% | 22ms | 620 | 0 |\n"
                "| EU-West | 99.97% | 31ms | 540 | 1 |\n"
                "| APAC-JP | 99.96% | 34ms | 380 | 1 |\n"
                "| NA-West | 99.82% | 26ms | 420 | 3 |\n\n"
                "**Alert:** NA-West experienced 3 incidents this week, with MTTR averaging 14 minutes. "
                "Root cause traced to a load balancer misconfiguration during the Season 3 launch surge. "
                "Issue was patched Tuesday and stability has since returned to normal.\n\n"
                "**Recommendation:** Increase NA-West auto-scaling threshold from 80% to 70% CPU to "
                "handle future seasonal spikes."
            ),
            "sql": (
                "SELECT region, uptime_pct, avg_latency_ms, server_count,\n"
                "       incidents_7d, mttr_minutes\n"
                "FROM gaming_iq.gold.server_health_metrics\n"
                "ORDER BY uptime_pct DESC"
            ),
            "data": [
                {"region": "NA-East", "uptime": "99.99%", "latency_ms": 22, "servers": 620, "incidents": 0},
                {"region": "EU-West", "uptime": "99.97%", "latency_ms": 31, "servers": 540, "incidents": 1},
                {"region": "APAC-JP", "uptime": "99.96%", "latency_ms": 34, "servers": 380, "incidents": 1},
                {"region": "NA-West", "uptime": "99.82%", "latency_ms": 26, "servers": 420, "incidents": 3},
            ],
        },
    ],

    # -----------------------------------------------------------------
    # TELECOM
    # -----------------------------------------------------------------
    "telecom": [
        {
            "patterns": [r"churn", r"subscriber", r"nps", r"csat", r"customer"],
            "answer": (
                "**Subscriber Churn & Satisfaction Metrics**\n\n"
                "Total subscribers: **14.2M** | Monthly churn rate: **1.8%** (target: <2.0%)\n"
                "Net Promoter Score (NPS): **+32** | CSAT: **4.1/5.0**\n\n"
                "**Churn by Segment:**\n"
                "| Segment | Subscribers | Churn Rate | NPS | ARPU |\n"
                "|---------|------------|-----------|-----|------|\n"
                "| Enterprise | 420K | 0.6% | +48 | $142 |\n"
                "| Family Plans | 3.8M | 1.2% | +38 | $89 |\n"
                "| Individual | 6.1M | 2.1% | +28 | $62 |\n"
                "| Prepaid | 3.9M | 3.4% | +18 | $28 |\n\n"
                "**Key Churn Drivers (Churn Predictor model, AUC: 0.88):**\n"
                "1. Network quality complaints (SHAP: 0.31)\n"
                "2. Bill shock events (SHAP: 0.24)\n"
                "3. Competitor promotion exposure (SHAP: 0.19)\n\n"
                "**Recommendation:** Prepaid segment churn at 3.4% is above target. Proactive retention "
                "offers to the 18K subscribers flagged as high-risk could save $4.2M in annual revenue."
            ),
            "sql": (
                "SELECT segment, subscriber_count, churn_rate_pct, nps_score,\n"
                "       arpu\n"
                "FROM telecom_iq.gold.subscriber_metrics\n"
                "ORDER BY churn_rate_pct DESC"
            ),
            "data": [
                {"segment": "Enterprise", "subscribers": "420K", "churn_rate": "0.6%", "nps": "+48", "arpu": "$142"},
                {"segment": "Family Plans", "subscribers": "3.8M", "churn_rate": "1.2%", "nps": "+38", "arpu": "$89"},
                {"segment": "Individual", "subscribers": "6.1M", "churn_rate": "2.1%", "nps": "+28", "arpu": "$62"},
                {"segment": "Prepaid", "subscribers": "3.9M", "churn_rate": "3.4%", "nps": "+18", "arpu": "$28"},
            ],
        },
        {
            "patterns": [r"network", r"uptime", r"capacity", r"5g", r"outage"],
            "answer": (
                "**Network Operations & Capacity Report**\n\n"
                "Overall network uptime: **99.97%** | 5G coverage: **78%** of service area\n"
                "Total cell sites: **48,200** | Active outages: **3**\n\n"
                "**Network Performance by Technology:**\n"
                "| Technology | Sites | Uptime | Avg Throughput | Utilization |\n"
                "|-----------|-------|--------|---------------|-------------|\n"
                "| 5G mmWave | 8,400 | 99.98% | 1.2 Gbps | 42% |\n"
                "| 5G Sub-6 | 14,800 | 99.97% | 380 Mbps | 61% |\n"
                "| 4G LTE | 18,600 | 99.96% | 85 Mbps | 78% |\n"
                "| 3G Legacy | 6,400 | 99.91% | 12 Mbps | 34% |\n\n"
                "**Active Outages:**\n"
                "1. Metro-NE cluster: 12 sites down since 06:42 UTC -- fiber cut, ETA 4 hours\n"
                "2. Rural-SW site #4821: power failure -- generator deployed, ETA 2 hours\n"
                "3. Urban-Central #1204: RAN software crash -- remote restart in progress\n\n"
                "**Recommendation:** 4G LTE utilization at 78% is approaching capacity threshold. "
                "Accelerate 5G migration in the top 20 high-traffic LTE sites to offload 15-20% of traffic."
            ),
            "sql": (
                "SELECT technology, site_count, uptime_pct, avg_throughput_mbps,\n"
                "       utilization_pct\n"
                "FROM telecom_iq.gold.network_performance\n"
                "ORDER BY utilization_pct DESC"
            ),
            "data": [
                {"technology": "5G mmWave", "sites": 8400, "uptime": "99.98%", "throughput": "1.2 Gbps", "utilization": "42%"},
                {"technology": "5G Sub-6", "sites": 14800, "uptime": "99.97%", "throughput": "380 Mbps", "utilization": "61%"},
                {"technology": "4G LTE", "sites": 18600, "uptime": "99.96%", "throughput": "85 Mbps", "utilization": "78%"},
                {"technology": "3G Legacy", "sites": 6400, "uptime": "99.91%", "throughput": "12 Mbps", "utilization": "34%"},
            ],
        },
        {
            "patterns": [r"fraud", r"sim", r"swap", r"irsf"],
            "answer": (
                "**Telecom Fraud Detection Summary**\n\n"
                "Total fraud events blocked (30d): **4,847** | Estimated savings: **$2.8M**\n"
                "Fraud detection rate: **97.2%** | False positive rate: **0.8%**\n\n"
                "**Fraud by Category:**\n"
                "| Category | Events | Value Blocked | Trend |\n"
                "|----------|--------|-------------|-------|\n"
                "| SIM Swap | 1,240 | $980K | Up 14% |\n"
                "| IRSF (Revenue Share) | 890 | $720K | Down 8% |\n"
                "| Subscription Fraud | 1,420 | $640K | Stable |\n"
                "| Wangiri (Callback) | 780 | $310K | Up 22% |\n"
                "| Account Takeover | 517 | $150K | Down 5% |\n\n"
                "**Alert:** SIM Swap fraud is up 14% month-over-month, with a new pattern detected: "
                "coordinated attacks targeting port-out requests during overnight hours (01:00-05:00 local). "
                "The Fraud Detection model flagged 89% of these before completion.\n\n"
                "**Recommendation:** Implement mandatory biometric verification for SIM swap requests "
                "during off-hours. Estimated reduction: 40% of overnight SIM swap fraud."
            ),
            "sql": (
                "SELECT fraud_category, event_count, value_blocked,\n"
                "       mom_trend_pct\n"
                "FROM telecom_iq.gold.fraud_detection_metrics\n"
                "WHERE period = 'last_30d'\n"
                "ORDER BY value_blocked DESC"
            ),
            "data": [
                {"category": "SIM Swap", "events": 1240, "value_blocked": "$980K", "trend": "+14%"},
                {"category": "IRSF", "events": 890, "value_blocked": "$720K", "trend": "-8%"},
                {"category": "Subscription Fraud", "events": 1420, "value_blocked": "$640K", "trend": "0%"},
                {"category": "Wangiri", "events": 780, "value_blocked": "$310K", "trend": "+22%"},
            ],
        },
    ],

    # -----------------------------------------------------------------
    # MEDIA
    # -----------------------------------------------------------------
    "media": [
        {
            "patterns": [r"viewer", r"audience", r"watch", r"engagement"],
            "answer": (
                "**Audience & Engagement Metrics**\n\n"
                "Monthly Active Viewers: **28.4M** (+6.2% MoM)\n"
                "Avg watch time per session: **42 minutes** | Completion rate: **68%**\n\n"
                "**Audience by Platform:**\n"
                "| Platform | MAV | Avg Watch Time | Engagement Rate |\n"
                "|----------|-----|---------------|----------------|\n"
                "| Connected TV | 12.8M | 58 min | 74% |\n"
                "| Mobile | 9.2M | 32 min | 62% |\n"
                "| Desktop | 4.1M | 41 min | 71% |\n"
                "| Tablet | 2.3M | 47 min | 69% |\n\n"
                "**Engagement Trends:**\n"
                "Connected TV continues to dominate with the highest watch time and engagement rate. "
                "Mobile viewers are growing fastest (+11% MoM) but have shorter sessions. "
                "The new recommendation engine improved completion rates by 4.2 percentage points "
                "across all platforms since launch.\n\n"
                "**Recommendation:** Invest in mobile-optimized short-form content (5-15 min) to "
                "increase mobile engagement rate, which trails CTV by 12 percentage points."
            ),
            "sql": (
                "SELECT platform, monthly_active_viewers, avg_watch_time_min,\n"
                "       engagement_rate_pct, mom_growth_pct\n"
                "FROM media_iq.gold.audience_metrics\n"
                "ORDER BY monthly_active_viewers DESC"
            ),
            "data": [
                {"platform": "Connected TV", "mav": "12.8M", "watch_time": "58 min", "engagement": "74%"},
                {"platform": "Mobile", "mav": "9.2M", "watch_time": "32 min", "engagement": "62%"},
                {"platform": "Desktop", "mav": "4.1M", "watch_time": "41 min", "engagement": "71%"},
                {"platform": "Tablet", "mav": "2.3M", "watch_time": "47 min", "engagement": "69%"},
            ],
        },
        {
            "patterns": [r"content", r"roi", r"catalog", r"recommend"],
            "answer": (
                "**Content Performance & ROI Analysis**\n\n"
                "Total catalog titles: **14,200** | Active this month: **8,400**\n"
                "Content investment (Q1): **$48M** | Revenue generated: **$127M** | ROI: **2.65x**\n\n"
                "**Top Performing Content Categories:**\n"
                "| Category | Titles | Streams | Revenue | ROI |\n"
                "|----------|--------|---------|---------|-----|\n"
                "| Original Drama | 42 | 84M | $38M | 3.8x |\n"
                "| Licensed Film | 1,200 | 62M | $31M | 2.4x |\n"
                "| Live Sports | 28 | 48M | $28M | 2.1x |\n"
                "| Docuseries | 86 | 31M | $14M | 4.2x |\n"
                "| Kids & Family | 340 | 28M | $8M | 3.1x |\n\n"
                "**Content Recommendation Engine:**\n"
                "The ML-powered recommendation system drives **34%** of all content starts. "
                "Personalized recommendations have a 2.8x higher completion rate than browse-initiated views. "
                "Docuseries have the highest ROI at 4.2x, suggesting increased investment in this category.\n\n"
                "**Recommendation:** Increase docuseries production budget by 25% -- highest ROI category "
                "with strong audience demand signals."
            ),
            "sql": (
                "SELECT content_category, title_count, total_streams,\n"
                "       revenue, roi_multiple\n"
                "FROM media_iq.gold.content_performance\n"
                "ORDER BY roi_multiple DESC"
            ),
            "data": [
                {"category": "Original Drama", "titles": 42, "streams": "84M", "revenue": "$38M", "roi": "3.8x"},
                {"category": "Licensed Film", "titles": 1200, "streams": "62M", "revenue": "$31M", "roi": "2.4x"},
                {"category": "Live Sports", "titles": 28, "streams": "48M", "revenue": "$28M", "roi": "2.1x"},
                {"category": "Docuseries", "titles": 86, "streams": "31M", "revenue": "$14M", "roi": "4.2x"},
            ],
        },
        {
            "patterns": [r"ad", r"cpm", r"fill", r"roas", r"campaign"],
            "answer": (
                "**Advertising Performance & Yield**\n\n"
                "Total ad revenue (MTD): **$8.4M** | Avg CPM: **$24.80** | Fill rate: **94.2%**\n"
                "Active campaigns: **342** | Avg ROAS: **4.2x**\n\n"
                "**Ad Performance by Format:**\n"
                "| Format | Impressions | CPM | Fill Rate | Revenue |\n"
                "|--------|------------|-----|-----------|--------|\n"
                "| Pre-roll (15s) | 48M | $28.40 | 96% | $1.36M |\n"
                "| Mid-roll (30s) | 32M | $34.20 | 92% | $1.09M |\n"
                "| Display Banner | 120M | $8.60 | 97% | $1.03M |\n"
                "| Sponsored Content | 8M | $62.00 | 88% | $0.50M |\n"
                "| Interactive Overlay | 18M | $42.10 | 85% | $0.76M |\n\n"
                "**Campaign Highlights:**\n"
                "Top performing campaign: Auto manufacturer Q1 launch -- ROAS of 6.8x with "
                "interactive overlay format. Mid-roll 30s slots command the highest CPM at $34.20 but "
                "have the second-lowest fill rate.\n\n"
                "**Recommendation:** Increase interactive overlay inventory -- highest CPM growth "
                "(+18% QoQ) with strong advertiser demand. Address mid-roll fill rate gap by offering "
                "programmatic backfill."
            ),
            "sql": (
                "SELECT ad_format, impressions, cpm, fill_rate_pct,\n"
                "       revenue\n"
                "FROM media_iq.gold.ad_performance\n"
                "ORDER BY cpm DESC"
            ),
            "data": [
                {"format": "Pre-roll (15s)", "impressions": "48M", "cpm": "$28.40", "fill_rate": "96%", "revenue": "$1.36M"},
                {"format": "Mid-roll (30s)", "impressions": "32M", "cpm": "$34.20", "fill_rate": "92%", "revenue": "$1.09M"},
                {"format": "Display Banner", "impressions": "120M", "cpm": "$8.60", "fill_rate": "97%", "revenue": "$1.03M"},
                {"format": "Sponsored Content", "impressions": "8M", "cpm": "$62.00", "fill_rate": "88%", "revenue": "$0.50M"},
            ],
        },
    ],

    # -----------------------------------------------------------------
    # FINANCIAL SERVICES
    # -----------------------------------------------------------------
    "financial_services": [
        {
            "patterns": [r"fraud", r"transaction", r"aml", r"blocked"],
            "answer": (
                "**Fraud Detection & AML Screening**\n\n"
                "Current fraud detection rate: **99.77%** (vs. 99.71% last month)\n"
                "Transactions today: **12.5M** | Fraud blocked: **847** | Value saved: **$2.9M**\n"
                "False positive rate: **0.40%** (improved from 0.52%)\n\n"
                "**Fraud by Channel:**\n"
                "| Channel | Blocked | Value | Detection Rate |\n"
                "|---------|---------|-------|---------------|\n"
                "| Digital Banking | 412 | $1.4M | 99.82% |\n"
                "| Card Present | 198 | $680K | 99.91% |\n"
                "| Wire/ACH | 142 | $520K | 99.68% |\n"
                "| Mobile Wallet | 95 | $300K | 99.74% |\n\n"
                "**AML Screening:**\n"
                "The model flagged **89 suspicious patterns** for AML investigation, including "
                "34 structuring cases (deposits just under $10K threshold) and 28 rapid fund movements "
                "across 3+ accounts within 1 hour. All cases submitted to the SAR workflow.\n\n"
                "**Recommendation:** The recent model retrain incorporating device fingerprinting features "
                "reduced false positives by 23%, saving ~$1.2M in manual review costs."
            ),
            "sql": (
                "SELECT channel, blocked_count, value_saved,\n"
                "       detection_rate_pct\n"
                "FROM financial_services_iq.gold.fraud_detection_metrics\n"
                "WHERE snapshot_date = current_date()\n"
                "ORDER BY value_saved DESC"
            ),
            "data": [
                {"channel": "Digital Banking", "blocked": 412, "value_saved": "$1.4M", "detection_rate": "99.82%"},
                {"channel": "Card Present", "blocked": 198, "value_saved": "$680K", "detection_rate": "99.91%"},
                {"channel": "Wire/ACH", "blocked": 142, "value_saved": "$520K", "detection_rate": "99.68%"},
                {"channel": "Mobile Wallet", "blocked": 95, "value_saved": "$300K", "detection_rate": "99.74%"},
            ],
        },
        {
            "patterns": [r"portfolio", r"var", r"sharpe", r"alpha", r"market"],
            "answer": (
                "**Portfolio & Capital Markets Analysis**\n\n"
                "Total AUM: **$24.5B** | Active Positions: **8,450**\n"
                "Sharpe Ratio: **1.42** | Portfolio Beta: **0.94** | Alpha: **+2.1%** YTD\n\n"
                "**Value at Risk (VaR):**\n"
                "| Scenario | VaR (95%) | VaR (99%) | Expected Shortfall |\n"
                "|----------|----------|----------|-------------------|\n"
                "| Normal | $47M | $72M | $89M |\n"
                "| Moderate Stress | $94M | $144M | $178M |\n"
                "| Severe Stress | $235M | $361M | $445M |\n\n"
                "**Asset Class Performance:**\n"
                "| Asset Class | AUM | YTD Return | Sharpe |\n"
                "|-----------|-----|-----------|--------|\n"
                "| US Equities | $9.8B | +8.4% | 1.62 |\n"
                "| Fixed Income | $7.2B | +3.1% | 1.18 |\n"
                "| Alternatives | $4.1B | +12.2% | 1.84 |\n"
                "| International | $3.4B | +5.7% | 1.21 |\n\n"
                "The portfolio is well-diversified with a beta below 1.0. Under severe stress, maximum "
                "drawdown is estimated at 1.47% of AUM. Current VaR levels are within risk appetite."
            ),
            "sql": (
                "SELECT asset_class, aum, ytd_return_pct, sharpe_ratio,\n"
                "       var_95, var_99\n"
                "FROM financial_services_iq.gold.portfolio_market_risk\n"
                "WHERE snapshot_date = current_date()\n"
                "ORDER BY aum DESC"
            ),
            "data": [
                {"asset_class": "US Equities", "aum": "$9.8B", "ytd_return": "+8.4%", "sharpe": 1.62},
                {"asset_class": "Fixed Income", "aum": "$7.2B", "ytd_return": "+3.1%", "sharpe": 1.18},
                {"asset_class": "Alternatives", "aum": "$4.1B", "ytd_return": "+12.2%", "sharpe": 1.84},
                {"asset_class": "International", "aum": "$3.4B", "ytd_return": "+5.7%", "sharpe": 1.21},
            ],
        },
        {
            "patterns": [r"insurance", r"claims", r"combined", r"loss", r"underwriting"],
            "answer": (
                "**Insurance Analytics Overview**\n\n"
                "Combined ratio: **94.2%** (target: <96%) | Loss ratio: **62.8%** | Expense ratio: **31.4%**\n"
                "Gross written premium (YTD): **$3.8B** | Claims paid: **$2.4B**\n\n"
                "**Claims Analysis by Line of Business:**\n"
                "| Line of Business | GWP | Loss Ratio | Claims Freq | Avg Severity |\n"
                "|-----------------|-----|-----------|------------|-------------|\n"
                "| Auto | $1.4B | 68.2% | 8.4% | $12,400 |\n"
                "| Property | $1.1B | 58.4% | 3.2% | $34,200 |\n"
                "| Commercial Lines | $820M | 61.7% | 2.1% | $87,600 |\n"
                "| Workers Comp | $480M | 64.1% | 5.8% | $18,900 |\n\n"
                "**Underwriting Insights:**\n"
                "Auto loss ratio at 68.2% is above target of 65%. The Claims Severity Predictor "
                "model identifies distracted driving claims as the fastest-growing category (+18% YoY). "
                "Property claims are well-controlled with catastrophe reserves adequate for the season.\n\n"
                "**Recommendation:** Tighten auto underwriting in the top 5 loss-producing states "
                "and increase telematics discount to incentivize safer driving behavior."
            ),
            "sql": (
                "SELECT line_of_business, gwp, loss_ratio_pct, claims_frequency,\n"
                "       avg_severity\n"
                "FROM financial_services_iq.gold.insurance_analytics\n"
                "ORDER BY gwp DESC"
            ),
            "data": [
                {"lob": "Auto", "gwp": "$1.4B", "loss_ratio": "68.2%", "claims_freq": "8.4%", "avg_severity": "$12,400"},
                {"lob": "Property", "gwp": "$1.1B", "loss_ratio": "58.4%", "claims_freq": "3.2%", "avg_severity": "$34,200"},
                {"lob": "Commercial Lines", "gwp": "$820M", "loss_ratio": "61.7%", "claims_freq": "2.1%", "avg_severity": "$87,600"},
                {"lob": "Workers Comp", "gwp": "$480M", "loss_ratio": "64.1%", "claims_freq": "5.8%", "avg_severity": "$18,900"},
            ],
        },
        {
            "patterns": [r"credit", r"delinquency", r"loan", r"default", r"risk"],
            "answer": (
                "**Credit Risk & Delinquency Report**\n\n"
                "Portfolio value: **$3.4B** | Avg credit score: **698** | Overall delinquency: **4.1%**\n"
                "Model: Credit_Default_Predictor (GradientBoosting, AUC-ROC: 0.88)\n\n"
                "**Delinquency by Segment:**\n"
                "| Segment | 30d Delinquency | Trend (90d) | Accounts | Exposure |\n"
                "|---------|----------------|-------------|----------|----------|\n"
                "| Small Business | 5.8% | +1.2 pp | 3,400 | $890M |\n"
                "| Young Professionals | 4.2% | +0.8 pp | 8,900 | $1.2B |\n"
                "| Retail Banking | 3.2% | +0.3 pp | 42,000 | $4.8B |\n"
                "| Wealth Management | 0.8% | -0.1 pp | 2,100 | $6.2B |\n\n"
                "**Default Probability Distribution:**\n"
                "| PD Range | Accounts | Exposure | Expected Loss |\n"
                "|----------|----------|----------|---------------|\n"
                "| 0-1% (Prime) | 4,200 | $1.8B | $9.0M |\n"
                "| 1-5% (Standard) | 2,800 | $1.1B | $27.5M |\n"
                "| 5-15% (Subprime) | 680 | $380M | $28.5M |\n"
                "| >15% (High Risk) | 120 | $120M | $24.0M |\n\n"
                "Small Business has the fastest growing delinquency (+1.2 pp over 90 days). "
                "The subprime and high-risk segments represent 10% of accounts but 59% of expected losses. "
                "Recommend tightening origination criteria for DTI ratios above 45%."
            ),
            "sql": (
                "SELECT customer_segment, delinquency_rate_30d,\n"
                "       delinquency_trend_90d, account_count, total_exposure\n"
                "FROM financial_services_iq.gold.credit_risk_portfolio\n"
                "ORDER BY delinquency_trend_90d DESC"
            ),
            "data": [
                {"segment": "Small Business", "delinquency": "5.8%", "trend": "+1.2 pp", "accounts": 3400, "exposure": "$890M"},
                {"segment": "Young Professionals", "delinquency": "4.2%", "trend": "+0.8 pp", "accounts": 8900, "exposure": "$1.2B"},
                {"segment": "Retail Banking", "delinquency": "3.2%", "trend": "+0.3 pp", "accounts": 42000, "exposure": "$4.8B"},
                {"segment": "Wealth Management", "delinquency": "0.8%", "trend": "-0.1 pp", "accounts": 2100, "exposure": "$6.2B"},
            ],
        },
    ],

    # -----------------------------------------------------------------
    # HLS (Health & Life Sciences)
    # -----------------------------------------------------------------
    "hls": [
        {
            "patterns": [r"bed", r"utilization", r"admission", r"ed", r"wait", r"capacity"],
            "answer": (
                "**Provider Operations & Capacity Report**\n\n"
                "Current overall bed utilization: **87.3%** | ED avg wait: **34 min**\n"
                "Total admissions today: **285** | ED volume: **89 patients**\n\n"
                "**Facility Utilization:**\n"
                "| Facility | Bed Util | ED Wait | Admissions | Status |\n"
                "|----------|---------|---------|-----------|--------|\n"
                "| Metro General Hospital | 91.2% | 42 min | 142 | Near Capacity |\n"
                "| Westside Medical Center | 84.1% | 28 min | 98 | Normal |\n"
                "| Eastview Community Clinic | 72.5% | 22 min | 45 | Normal |\n\n"
                "**ED Wait Time by Department:**\n"
                "| Department | Avg Wait | Volume | Trend |\n"
                "|-----------|----------|--------|-------|\n"
                "| Orthopedics | 52 min | 28 | Up 15% |\n"
                "| Cardiology | 45 min | 34 | Stable |\n"
                "| Emergency | 38 min | 89 | Up 8% |\n"
                "| Pediatrics | 24 min | 41 | Stable |\n\n"
                "**Alert:** Metro General is projected to reach 95.8% capacity within 48 hours based on "
                "current admission trends. Recommend activating surge protocols and diverting non-critical "
                "cases to Eastview."
            ),
            "sql": (
                "SELECT facility, bed_utilization_pct, ed_avg_wait_min,\n"
                "       admissions_today, capacity_status\n"
                "FROM hls_iq.gold.patient_flow_metrics\n"
                "WHERE snapshot_time >= current_date()\n"
                "ORDER BY bed_utilization_pct DESC"
            ),
            "data": [
                {"facility": "Metro General Hospital", "bed_util": "91.2%", "ed_wait": "42 min", "admissions": 142, "status": "Near Capacity"},
                {"facility": "Westside Medical Center", "bed_util": "84.1%", "ed_wait": "28 min", "admissions": 98, "status": "Normal"},
                {"facility": "Eastview Community Clinic", "bed_util": "72.5%", "ed_wait": "22 min", "admissions": 45, "status": "Normal"},
            ],
        },
        {
            "patterns": [r"readmission", r"quality", r"sepsis", r"hcahps", r"clinical"],
            "answer": (
                "**Clinical Quality & Readmission Metrics**\n\n"
                "30-day readmission rate: **11.8%** (national avg: 13.9%)\n"
                "HCAHPS overall score: **4.2/5.0** | Sepsis bundle compliance: **94%**\n\n"
                "**Readmission Risk Analysis:**\n"
                "Model: Readmission_Risk_Predictor (AUC-ROC: 0.89)\n"
                "Discharges analyzed (last 48h): **127 patients**\n\n"
                "| Risk Level | Patients | Action Required |\n"
                "|-----------|----------|----------------|\n"
                "| High (>70%) | 18 | Immediate follow-up |\n"
                "| Medium (40-70%) | 34 | 7-day check-in |\n"
                "| Low (<40%) | 75 | Standard protocol |\n\n"
                "**Top Quality Metrics:**\n"
                "| Metric | Score | Target | Status |\n"
                "|--------|-------|--------|--------|\n"
                "| Sepsis Bundle Compliance | 94% | 90% | Exceeding |\n"
                "| CLABSI Rate | 0.42 | <0.50 | On Target |\n"
                "| Falls per 1000 pt-days | 2.1 | <2.5 | On Target |\n"
                "| HCAHPS Communication | 4.4 | 4.0 | Exceeding |\n\n"
                "**Key Insight:** Patients with 3+ prior admissions in 12 months and comorbidity index >3.5 "
                "have an 82% readmission rate. This cohort represents 23% of cardiology discharges. "
                "Targeted transitional care programs for this group could reduce readmissions by 15-20%."
            ),
            "sql": (
                "SELECT quality_metric, current_score, target, status,\n"
                "       trend_30d\n"
                "FROM hls_iq.gold.clinical_quality_metrics\n"
                "ORDER BY status DESC"
            ),
            "data": [
                {"metric": "Readmission Rate (30d)", "score": "11.8%", "target": "<13.9%", "status": "Exceeding"},
                {"metric": "Sepsis Bundle Compliance", "score": "94%", "target": "90%", "status": "Exceeding"},
                {"metric": "CLABSI Rate", "score": "0.42", "target": "<0.50", "status": "On Target"},
                {"metric": "HCAHPS Overall", "score": "4.2/5.0", "target": "4.0", "status": "Exceeding"},
            ],
        },
        {
            "patterns": [r"claims", r"mlr", r"prior", r"auth", r"fwa", r"plan"],
            "answer": (
                "**Health Plan Analytics & Claims Intelligence**\n\n"
                "Medical Loss Ratio (MLR): **84.2%** (target: 80-85%)\n"
                "Claims processed (MTD): **2.4M** | Avg turnaround: **4.2 days**\n"
                "Prior authorization volume: **128K** | Auto-approval rate: **62%**\n\n"
                "**Claims Performance:**\n"
                "| Metric | Value | Target | Status |\n"
                "|--------|-------|--------|--------|\n"
                "| Clean claims rate | 94.8% | 95% | Near Target |\n"
                "| First-pass resolution | 87.2% | 85% | Exceeding |\n"
                "| Denial rate | 8.4% | <10% | On Target |\n"
                "| Avg days to payment | 18.2 | <21 | On Target |\n\n"
                "**FWA (Fraud, Waste & Abuse) Detection:**\n"
                "The FWA model flagged **342 suspicious claims** this month, totaling $4.8M in "
                "potential savings. Top patterns:\n"
                "- Upcoding: 142 claims ($2.1M)\n"
                "- Unbundling: 98 claims ($1.4M)\n"
                "- Duplicate billing: 67 claims ($820K)\n"
                "- Phantom billing: 35 claims ($480K)\n\n"
                "**Prior Auth Optimization:** The AI-powered prior auth system auto-approves 62% of "
                "requests in <2 hours, up from 41% last quarter. This has reduced provider abrasion "
                "scores by 28% and saved 4,200 staff hours per month.\n\n"
                "**Recommendation:** Increase auto-approval threshold for low-risk procedure codes "
                "to target 70% auto-approval rate by Q3."
            ),
            "sql": (
                "SELECT metric_name, current_value, target_value, status,\n"
                "       trend\n"
                "FROM hls_iq.gold.claims_analytics\n"
                "WHERE report_period = current_date()\n"
                "ORDER BY metric_name"
            ),
            "data": [
                {"metric": "Medical Loss Ratio", "value": "84.2%", "target": "80-85%", "status": "On Target"},
                {"metric": "Clean Claims Rate", "value": "94.8%", "target": "95%", "status": "Near Target"},
                {"metric": "Prior Auth Auto-Approval", "value": "62%", "target": "60%", "status": "Exceeding"},
                {"metric": "FWA Savings (MTD)", "value": "$4.8M", "target": "$4M", "status": "Exceeding"},
            ],
        },
    ],

    # -----------------------------------------------------------------
    # MANUFACTURING
    # -----------------------------------------------------------------
    "manufacturing": [
        {
            "patterns": [r"oee", r"equipment", r"effectiveness", r"utilization", r"downtime"],
            "answer": (
                "**Overall Equipment Effectiveness (OEE) Analysis**\n\n"
                "Current OEE across all facilities: **87.4%** (+2.1% vs last month)\n\n"
                "**OEE Breakdown by Facility:**\n"
                "| Facility | Availability | Performance | Quality | OEE |\n"
                "|----------|-------------|-------------|---------|-----|\n"
                "| Berlin | 94.2% | 91.8% | 98.1% | 84.8% |\n"
                "| Detroit | 92.8% | 93.4% | 97.6% | 84.6% |\n"
                "| Tokyo | 96.1% | 94.2% | 98.8% | 89.5% |\n"
                "| Shanghai | 93.5% | 95.1% | 97.2% | 86.5% |\n\n"
                "**Key Insight:** Tokyo leads with 89.5% OEE driven by superior availability "
                "(96.1%) and quality (98.8%). Shanghai has the highest performance rate but "
                "quality improvements could push OEE above 90%.\n\n"
                "**Unplanned Downtime:** 1.4 hrs/week avg (down from 2.1 hrs last quarter)"
            ),
            "sql": (
                "SELECT facility, availability_pct, performance_pct, quality_pct,\n"
                "       (availability_pct * performance_pct * quality_pct / 10000) AS oee\n"
                "FROM manufacturing_iq.gold.equipment_metrics\n"
                "WHERE report_date = current_date()\n"
                "GROUP BY facility\n"
                "ORDER BY oee DESC"
            ),
        },
        {
            "patterns": [r"quality", r"defect", r"yield", r"scrap", r"first pass"],
            "answer": (
                "**Quality Control Summary**\n\n"
                "- **First Pass Yield:** 94.8% (+0.6% vs last month)\n"
                "- **DPMO:** 2,340 (target: 2,000)\n"
                "- **Sigma Level:** 4.32\u03c3\n"
                "- **Scrap Rate:** 2.3% ($142K MTD)\n\n"
                "**Defects by Type:**\n"
                "| Type | Count | % of Total | Trend |\n"
                "|------|-------|------------|-------|\n"
                "| Dimensional | 46 | 32% | \u2193 -8% |\n"
                "| Surface Finish | 50 | 34% | \u2191 +12% |\n"
                "| Structural | 19 | 13% | \u2193 -3% |\n"
                "| Cosmetic | 31 | 21% | \u2192 flat |\n\n"
                "**Alert:** Welding-C1 line in Tokyo shows 34% increase in surface finish defects. "
                "Root cause: worn tooling on station WC1-07."
            ),
            "sql": (
                "SELECT defect_type, COUNT(*) AS defect_count,\n"
                "       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct\n"
                "FROM manufacturing_iq.gold.quality_events\n"
                "WHERE event_date >= date_sub(current_date(), 30)\n"
                "GROUP BY defect_type ORDER BY defect_count DESC"
            ),
        },
        {
            "patterns": [r"maintenance", r"predict", r"failure", r"mtbf", r"mttr", r"uptime"],
            "answer": (
                "**Predictive Maintenance Dashboard**\n\n"
                "- **MTBF (Mean Time Between Failures):** 842 hours\n"
                "- **MTTR (Mean Time to Repair):** 4.2 hours\n"
                "- **Equipment Availability:** 97.2% (+1.8% vs prior quarter)\n"
                "- **Open Work Orders:** 23\n\n"
                "**Upcoming Predicted Failures (7-day window):**\n"
                "| Equipment | Facility | Risk Score | Predicted Failure |\n"
                "|-----------|----------|------------|-------------------|\n"
                "| CNC-A1 Spindle | Berlin | 94% | 2-3 days |\n"
                "| Hydraulic Press HP-03 | Detroit | 87% | 4-5 days |\n"
                "| Welding Robot WR-07 | Tokyo | 82% | 5-7 days |\n\n"
                "**Model Performance:** The XGBoost failure predictor achieves 92% accuracy "
                "with a 78% confidence score on 3-week forecasts."
            ),
            "sql": (
                "SELECT equipment_id, facility, risk_score,\n"
                "       predicted_failure_date, confidence\n"
                "FROM manufacturing_iq.gold.predictive_maintenance\n"
                "WHERE predicted_failure_date <= date_add(current_date(), 7)\n"
                "ORDER BY risk_score DESC"
            ),
        },
        {
            "patterns": [r"supply", r"supplier", r"lead time", r"inventory", r"shortage"],
            "answer": (
                "**Supply Chain Risk Summary**\n\n"
                "- **Active Suppliers:** 148\n"
                "- **On-Time Delivery:** 91.3%\n"
                "- **Average Lead Time:** 18.4 days\n"
                "- **Open Risks:** 12 (2 critical, 4 warning)\n\n"
                "**Critical Alerts:**\n"
                "1. **Semiconductor Shortage (NXP):** Lead time extended to 26 weeks. "
                "Buffer stock covers 3.5 weeks. Alt supplier qualification in progress.\n"
                "2. **Shanghai Port Congestion:** 8-12 day vessel delays. 3 containers held. "
                "Air freight authorized for critical components.\n\n"
                "**Inventory Health:**\n"
                "| Material | Stock (days) | Status |\n"
                "|----------|-------------|--------|\n"
                "| MCU Modules | 24 | Critical |\n"
                "| Aluminum 6061 | 18 | Warning |\n"
                "| Steel AISI 4140 | 45 | Healthy |\n"
                "| Hydraulic Valves | 12 | Critical |"
            ),
            "sql": (
                "SELECT material, stock_days, risk_status,\n"
                "       supplier, lead_time_days\n"
                "FROM manufacturing_iq.gold.supply_chain_inventory\n"
                "WHERE stock_days < 30\n"
                "ORDER BY stock_days ASC"
            ),
        },
        {
            "patterns": [r"energy", r"carbon", r"sustain", r"emission", r"waste"],
            "answer": (
                "**Energy & Sustainability Metrics**\n\n"
                "- **Energy Efficiency Index:** 82.5% (target: 85%)\n"
                "- **Carbon Emissions:** 315 tonnes CO2e/month (\u2193 25% YoY)\n"
                "- **Water Usage:** 142 m\u00b3/day (\u2193 21% YoY)\n"
                "- **Waste Recycling Rate:** 73.8% (target: 80%)\n"
                "- **Sustainability Score:** B+\n\n"
                "**Energy per Unit by Facility:**\n"
                "| Facility | kWh/unit | Target | Status |\n"
                "|----------|----------|--------|--------|\n"
                "| Berlin | 4.8 | 4.5 | Over |\n"
                "| Detroit | 5.2 | 4.5 | Over |\n"
                "| Tokyo | 3.9 | 4.5 | Met |\n"
                "| Shanghai | 6.1 | 4.5 | Over |\n\n"
                "**Progress:** Scope 1 emissions reduced 68%, Scope 2 reduced 52%. "
                "On track for carbon neutrality target by 2030."
            ),
        },
    ],

    # -----------------------------------------------------------------
    # RISK
    # -----------------------------------------------------------------
    "risk": [
        {
            "patterns": [r"var", r"value at risk", r"market risk", r"trading", r"p&l"],
            "answer": (
                "**Market Risk / Value at Risk Analysis**\n\n"
                "- **VaR (95% 1-day):** $142M\n"
                "- **VaR (99% 1-day):** $218M\n"
                "- **Expected Shortfall (97.5%):** $287M\n"
                "- **Portfolio Beta:** 1.12\n\n"
                "**VaR by Asset Class:**\n"
                "| Asset Class | VaR ($M) | % of Total | Trend |\n"
                "|------------|----------|------------|-------|\n"
                "| Equities | $62M | 44% | \u2191 +8% |\n"
                "| Fixed Income | $31M | 22% | \u2193 -3% |\n"
                "| FX | $24M | 17% | \u2192 flat |\n"
                "| Commodities | $15M | 11% | \u2191 +12% |\n"
                "| Derivatives | $10M | 7% | \u2193 -5% |\n\n"
                "**Backtesting:** 3 exceptions in the last 250 trading days (within Basel "
                "green zone). The Market Anomaly Detector (IsolationForest) achieves 89% "
                "precision on intraday anomalies."
            ),
            "sql": (
                "SELECT asset_class, var_1d_99, var_contribution_pct,\n"
                "       var_change_30d\n"
                "FROM risk_iq.gold.market_risk_summary\n"
                "WHERE report_date = current_date()\n"
                "ORDER BY var_1d_99 DESC"
            ),
        },
        {
            "patterns": [r"credit", r"default", r"portfolio", r"loan", r"npl", r"exposure"],
            "answer": (
                "**Credit Risk Portfolio Summary**\n\n"
                "- **Total Exposure:** $48.3B\n"
                "- **Default Rate:** 1.24%\n"
                "- **Loss Given Default:** 42.8%\n"
                "- **NPL Ratio:** 2.1%\n"
                "- **Coverage Ratio:** 156%\n\n"
                "**Portfolio by Segment:**\n"
                "| Segment | Exposure | PD | LGD | Expected Loss |\n"
                "|---------|----------|-----|-----|---------------|\n"
                "| Retail Mortgages | $15.6B | 0.42% | 22% | $14.4M |\n"
                "| Corporate Lending | $12.1B | 0.85% | 38% | $39.1M |\n"
                "| Commercial RE | $8.4B | 2.30% | 45% | $86.9M |\n"
                "| Consumer Credit | $6.1B | 1.92% | 48% | $56.2M |\n"
                "| SME Portfolio | $4.2B | 3.10% | 52% | $67.7M |\n\n"
                "**Alert:** Commercial RE concentration exceeds 30% appetite threshold. "
                "Credit Default Predictor (XGBoost, AUC-ROC: 0.94) flags 847 accounts "
                "for watchlist review."
            ),
            "sql": (
                "SELECT portfolio_segment, total_exposure,\n"
                "       probability_of_default, loss_given_default,\n"
                "       expected_loss\n"
                "FROM risk_iq.gold.credit_portfolio_summary\n"
                "ORDER BY total_exposure DESC"
            ),
        },
        {
            "patterns": [r"operational", r"incident", r"loss", r"outage", r"fraud"],
            "answer": (
                "**Operational Risk Summary**\n\n"
                "- **Open Incidents:** 14\n"
                "- **Under Investigation:** 8\n"
                "- **Total Losses YTD:** $18.7M\n"
                "- **Avg Resolution Time:** 4.2 days\n\n"
                "**Active Critical Incidents:**\n"
                "1. **Core Banking Outage** (OPS-2024-0847): 3h 42m ongoing. "
                "~142K transactions affected. Failover active. Est. loss: $4.2M.\n"
                "2. **Unauthorized Wire Transfer** (OPS-2024-0846): $8.6M attempted "
                "from 3 dormant accounts. Blocked, forensics engaged.\n\n"
                "**Loss by Event Type (YTD):**\n"
                "| Event Type | Loss | % of Total |\n"
                "|-----------|------|------------|\n"
                "| Execution & Process | $7.2M | 39% |\n"
                "| External Fraud | $4.8M | 26% |\n"
                "| System Failures | $3.9M | 21% |\n"
                "| Employment Practices | $2.8M | 15% |"
            ),
            "sql": (
                "SELECT event_type, SUM(loss_amount) AS total_loss,\n"
                "       COUNT(*) AS incident_count\n"
                "FROM risk_iq.gold.operational_incidents\n"
                "WHERE discovery_date >= date_trunc('year', current_date())\n"
                "GROUP BY event_type ORDER BY total_loss DESC"
            ),
        },
        {
            "patterns": [r"compliance", r"regulat", r"basel", r"capital", r"cet1", r"audit"],
            "answer": (
                "**Regulatory Compliance Status**\n\n"
                "- **Overall Compliance Score:** 94.2%\n"
                "- **Open Findings:** 23\n"
                "- **CET1 Capital Ratio:** 14.1% (min: 4.5%)\n"
                "- **Total Capital Ratio:** 17.3% (min: 8.0%)\n"
                "- **Leverage Ratio:** 6.2% (min: 3.0%)\n"
                "- **LCR:** 128% (min: 100%)\n\n"
                "**Compliance Items by Status:**\n"
                "| Status | Count | % |\n"
                "|--------|-------|---|\n"
                "| Compliant | 142 | 83.5% |\n"
                "| Under Review | 18 | 10.6% |\n"
                "| Remediation | 7 | 4.1% |\n"
                "| Past Due | 3 | 1.8% |\n\n"
                "**Critical Items:** AML/6AMLD enhanced due diligence (45% complete) "
                "and DORA ICT risk framework (35% complete) require immediate attention."
            ),
            "sql": (
                "SELECT regulation, compliance_pct, status, due_date\n"
                "FROM risk_iq.gold.compliance_tracker\n"
                "WHERE status != 'Compliant'\n"
                "ORDER BY compliance_pct ASC"
            ),
        },
        {
            "patterns": [r"cyber", r"threat", r"vulnerab", r"attack", r"security", r"patch"],
            "answer": (
                "**Cyber Risk Dashboard**\n\n"
                "- **Threat Level:** HIGH (78/100)\n"
                "- **Attack Frequency (24h):** 1,847 events\n"
                "- **Blocked Threats:** 1,694 (91.7% block rate)\n"
                "- **Avg Response Time:** 28 min (target: <30 min)\n"
                "- **Patch Compliance:** 91%\n\n"
                "**Open Vulnerabilities:**\n"
                "| Severity | Count | SLA Breached |\n"
                "|----------|-------|--------------|\n"
                "| Critical | 12 | 3 |\n"
                "| High | 47 | 8 |\n"
                "| Medium | 183 | 5 |\n"
                "| Low | 342 | 2 |\n"
                "| Informational | 89 | 0 |\n\n"
                "**Alert:** External threat level elevated since Mar 2 following "
                "threat intelligence briefing. Supply chain risk at 62% (elevated)."
            ),
            "sql": (
                "SELECT severity, COUNT(*) AS vuln_count,\n"
                "       SUM(CASE WHEN sla_breached THEN 1 ELSE 0 END) AS breached\n"
                "FROM risk_iq.gold.vulnerability_inventory\n"
                "WHERE status = 'open'\n"
                "GROUP BY severity ORDER BY vuln_count DESC"
            ),
        },
    ],
}


def _ask_demo(question: str, use_case: str) -> Dict[str, Any]:
    """Match the question against known patterns and return a demo response."""
    q_lower = question.lower()
    responses = _DEMO_RESPONSES.get(use_case, [])

    # Try to find a matching pattern
    best_match = None
    best_score = 0
    for resp in responses:
        score = 0
        for pattern in resp["patterns"]:
            if re.search(pattern, q_lower):
                score += 1
        if score > best_score:
            best_score = score
            best_match = resp

    if best_match and best_score > 0:
        return {
            "answer": best_match["answer"],
            "sql": best_match.get("sql"),
            "source": "demo",
            "data": best_match.get("data"),
        }

    # Fallback: generic response based on the vertical
    vertical_names = {
        "gaming": "player analytics and live game operations",
        "telecom": "network operations and subscriber management",
        "media": "audience intelligence and content analytics",
        "financial_services": "banking, capital markets, and insurance analytics",
        "hls": "healthcare providers, health plans, and biopharma analytics",
        "manufacturing": "production analytics, quality control, and supply chain management",
        "risk": "enterprise risk management, credit risk, and regulatory compliance",
    }
    vertical_desc = vertical_names.get(use_case, "your data")

    # Try to give a contextual generic answer
    try:
        from app.data_access import get_config
        cfg = get_config()
        sample_qs = cfg.get("genie", {}).get("sample_questions", [])
        tables = cfg.get("genie", {}).get("tables", [])
    except Exception:
        sample_qs = []
        tables = []

    suggestion_text = ""
    if sample_qs:
        suggestions = sample_qs[:3]
        suggestion_text = "\n\nHere are some questions I can answer in detail:\n" + \
            "\n".join(f"- {q}" for q in suggestions)

    table_text = ""
    if tables:
        table_text = f"\n\nI have access to these tables: {', '.join(t.split('.')[-1] for t in tables[:4])}."

    return {
        "answer": (
            f"I can help with questions about **{vertical_desc}**. "
            f"Your question \"{question}\" is interesting -- let me provide what I can.\n\n"
            f"In demo mode, I have pre-built responses for common analytical questions "
            f"about your data.{table_text}{suggestion_text}"
        ),
        "sql": None,
        "source": "demo",
        "data": None,
    }


# ===================================================================
#  Public API
# ===================================================================

def ask_genie(question: str, use_case: str) -> Dict[str, Any]:
    """Route a question to the best available backend.

    Always tries the Foundation Model first (databricks-claude-sonnet-4-6),
    falling back to demo mode if the FM call fails (e.g. no Databricks auth).

    Parameters
    ----------
    question : str
        The user's natural-language question.
    use_case : str
        The vertical identifier (e.g. 'gaming', 'hls').

    Returns
    -------
    dict
        {"answer": str, "sql": str|None, "source": str, "data": list|None}
    """
    # Always try the FM first
    try:
        return _ask_fm(question, use_case)
    except Exception as e:
        logger.warning("FM endpoint failed, falling back to demo mode: %s", e)

    # Fallback: Demo mode (always available)
    return _ask_demo(question, use_case)
