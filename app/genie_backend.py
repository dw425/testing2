"""
Genie AI chat backend for Blueprint IQ.

Routes questions to one of three backends:
  1. Databricks Genie API  (GENIE_SPACE_ID env var)
  2. Databricks Foundation Model  (DATABRICKS_FM_ENDPOINT env var)
  3. Demo mode  (fallback) -- keyword-matched responses per vertical

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
#  Backend selection helpers
# ===================================================================

def _get_genie_space_id() -> Optional[str]:
    return os.environ.get("GENIE_SPACE_ID")


def _get_fm_endpoint() -> Optional[str]:
    return os.environ.get("DATABRICKS_FM_ENDPOINT")


# ===================================================================
#  Backend 1: Databricks Genie API
# ===================================================================

def _ask_genie_api(question: str, use_case: str) -> Dict[str, Any]:
    """Call the Databricks Genie Space API and poll for a result."""
    from databricks.sdk import WorkspaceClient

    space_id = _get_genie_space_id()
    w = WorkspaceClient()

    # Start a conversation
    resp = w.genie.start_conversation(space_id=space_id, content=question)
    conversation_id = resp.conversation_id
    message_id = resp.message_id

    # Poll for completion (up to 60 seconds)
    for _ in range(30):
        time.sleep(2)
        try:
            msg = w.genie.get_message(space_id=space_id,
                                      conversation_id=conversation_id,
                                      message_id=message_id)
            if msg.status and msg.status.value in ("COMPLETED", "COMPLETED_WITH_ERROR"):
                break
        except Exception:
            continue

    # Extract answer and SQL from attachments
    answer_text = ""
    sql_text = None

    if hasattr(msg, "attachments") and msg.attachments:
        for att in msg.attachments:
            if hasattr(att, "text") and att.text:
                if hasattr(att.text, "content"):
                    answer_text += att.text.content + "\n"
            if hasattr(att, "query") and att.query:
                if hasattr(att.query, "query"):
                    sql_text = att.query.query
                    # Try to get query result
                    try:
                        result = w.genie.get_message_query_result(
                            space_id=space_id,
                            conversation_id=conversation_id,
                            message_id=message_id,
                            attachment_id=att.id,
                        )
                        if hasattr(result, "statement_response"):
                            sr = result.statement_response
                            if hasattr(sr, "result") and sr.result:
                                data_chunk = sr.result
                                if hasattr(data_chunk, "data_array"):
                                    columns = [c.name for c in sr.manifest.schema.columns] if hasattr(sr, "manifest") else []
                                    rows = []
                                    for row in data_chunk.data_array or []:
                                        rows.append(dict(zip(columns, row)) if columns else row)
                                    return {
                                        "answer": answer_text.strip() or "Here are the results from your Genie Space:",
                                        "sql": sql_text,
                                        "source": "genie",
                                        "data": rows[:20],
                                    }
                    except Exception as e:
                        logger.warning("Failed to get query result: %s", e)

    return {
        "answer": answer_text.strip() or "Genie processed your question but returned no text.",
        "sql": sql_text,
        "source": "genie",
        "data": None,
    }


# ===================================================================
#  Backend 2: Databricks Foundation Model
# ===================================================================

def _build_fm_system_prompt(use_case: str) -> str:
    """Build a system prompt with schema context from the config."""
    try:
        from app.data_access import get_config
        cfg = get_config()
    except Exception:
        cfg = {}

    genie_cfg = cfg.get("genie", {})
    tables = genie_cfg.get("tables", [])
    app_name = cfg.get("app", {}).get("name", "Blueprint IQ")

    # Build schema context from ML features and data config
    ml_cfg = cfg.get("ml", {})
    features_info = []
    for model_key, model_info in ml_cfg.items():
        if isinstance(model_info, dict) and "features" in model_info:
            features_info.append(
                f"  Model '{model_info.get('name', model_key)}' uses features: "
                f"{', '.join(model_info['features'])}"
            )

    table_list = "\n".join(f"  - {t}" for t in tables) if tables else "  (no tables configured)"
    features_list = "\n".join(features_info) if features_info else "  (no ML models configured)"

    return f"""You are {app_name} AI Assistant, an expert data analyst for the {use_case} vertical.
You answer questions about the data in the lakehouse and can generate SQL queries.

Available tables in Unity Catalog:
{table_list}

ML model features:
{features_list}

When answering:
- Be concise and data-driven
- If a SQL query would help answer the question, include it in your response prefixed with ```sql
- Reference specific tables and columns when relevant
- Provide actionable insights, not just raw numbers
"""


def _ask_fm(question: str, use_case: str) -> Dict[str, Any]:
    """Call a Databricks Foundation Model serving endpoint."""
    from databricks.sdk import WorkspaceClient

    endpoint = _get_fm_endpoint()
    w = WorkspaceClient()

    system_prompt = _build_fm_system_prompt(use_case)

    response = w.serving_endpoints.query(
        name=endpoint,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": question},
        ],
        max_tokens=1024,
        temperature=0.3,
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
#  Backend 3: Demo mode -- intelligent keyword-matched responses
# ===================================================================

_DEMO_RESPONSES: Dict[str, List[Dict[str, Any]]] = {
    # -----------------------------------------------------------------
    # MANUFACTURING
    # -----------------------------------------------------------------
    "manufacturing": [
        {
            "patterns": [r"anomal", r"vibrat", r"sensor", r"alert"],
            "answer": (
                "**Anomaly Detection Summary (Last 1 Hour)**\n\n"
                "We detected **87 anomalies** across all sites in the last hour. "
                "The most significant is on **CNC-Milling-BER-04** in Berlin, which is showing "
                "elevated vibration at **38.2 Hz** (normal range: 10-25 Hz) and temperature at "
                "**72.4C** (normal: 40-60C). The anomaly score is **0.94**, well above the 0.65 threshold.\n\n"
                "Root cause analysis points to **tool wear index** at 0.87, suggesting the cutting tool "
                "is due for replacement. SHAP analysis shows vibration_hz (0.34) and tool_wear_index (0.28) "
                "as the top contributing features.\n\n"
                "**Recommendation:** Schedule immediate tool replacement on BER-04 and inspect "
                "Lathe-DET-12 which shows early warning signs (score: 0.58)."
            ),
            "sql": (
                "SELECT machine_id, vibration_hz, temp_c, anomaly_score, tool_wear_index\n"
                "FROM manufacturing_iq.silver.cnc_anomalies\n"
                "WHERE anomaly_score > 0.65\n"
                "  AND timestamp >= current_timestamp() - INTERVAL 1 HOUR\n"
                "ORDER BY anomaly_score DESC\n"
                "LIMIT 10"
            ),
            "data": [
                {"machine_id": "CNC-Milling-BER-04", "vibration_hz": 38.2, "temp_c": 72.4, "anomaly_score": 0.94, "tool_wear_index": 0.87},
                {"machine_id": "Lathe-DET-12", "vibration_hz": 29.1, "temp_c": 63.8, "anomaly_score": 0.58, "tool_wear_index": 0.62},
                {"machine_id": "Assembly-Arm-BER-07", "vibration_hz": 26.4, "temp_c": 61.2, "anomaly_score": 0.42, "tool_wear_index": 0.51},
            ],
        },
        {
            "patterns": [r"inventory", r"stock", r"shortage", r"supply", r"forecast", r"part"],
            "answer": (
                "**Inventory Forecast (Next 30 Days)**\n\n"
                "Critical shortages predicted:\n"
                "- **Sensor Array X-12** (Tokyo): Only **0.5 days** of stock remaining (150 units, 300/day usage). "
                "URGENT -- production halt imminent.\n"
                "- **Precision Bearings** (Detroit): **3 days** remaining (2,100 units). Reorder triggered.\n"
                "- **Ceramic Insulator Plate** (Tokyo): **2 days** remaining (800 units).\n\n"
                "Healthy components:\n"
                "- CNC Alloy Block A (Berlin): 14 days remaining\n"
                "- Titanium Fastener Set (Berlin): 21 days remaining\n"
                "- Hydraulic Actuator B7 (Detroit): 7 days remaining\n\n"
                "**Recommendation:** Expedite Sensor Array X-12 order immediately. "
                "The Prophet forecasting model predicts Tokyo demand increasing 12% next quarter."
            ),
            "sql": (
                "SELECT component, site, current_stock, daily_usage,\n"
                "       current_stock / daily_usage AS days_remaining, status\n"
                "FROM manufacturing_iq.gold.site_component_status\n"
                "WHERE status IN ('Critical', 'Low')\n"
                "ORDER BY days_remaining ASC"
            ),
            "data": [
                {"component": "Sensor Array X-12", "site": "Tokyo", "current_stock": 150, "daily_usage": 300, "days_remaining": 0.5, "status": "Critical"},
                {"component": "Ceramic Insulator Plate", "site": "Tokyo", "current_stock": 800, "daily_usage": 400, "days_remaining": 2.0, "status": "Low"},
                {"component": "Precision Bearings", "site": "Detroit", "current_stock": 2100, "daily_usage": 700, "days_remaining": 3.0, "status": "Low"},
            ],
        },
        {
            "patterns": [r"quality", r"tolerance", r"inspect", r"spec", r"cpk", r"defect"],
            "answer": (
                "**Quality & Tolerance Report**\n\n"
                "In the last 24 hours, we performed **2,854,000 inspections** across all sites.\n"
                "- Out-of-spec count: **14** (0.0005% rate -- within target)\n"
                "- Process capability: Cp = **1.45**, Cpk = **1.38** (target: Cpk > 1.33)\n\n"
                "Site breakdown:\n"
                "- Berlin: 1.2M inspections, 4 OOS, Cpk 1.42\n"
                "- Detroit: 980K inspections, 6 OOS, Cpk 1.31 (below target)\n"
                "- Tokyo: 674K inspections, 4 OOS, Cpk 1.44\n\n"
                "**Alert:** Detroit's Cpk has dropped below the 1.33 threshold. "
                "Investigation shows Lathe-DET-12 dimensional drift correlating with the vibration anomaly."
            ),
            "sql": (
                "SELECT site, COUNT(*) AS inspections,\n"
                "       SUM(CASE WHEN out_of_spec THEN 1 ELSE 0 END) AS oos_count,\n"
                "       AVG(cpk) AS avg_cpk\n"
                "FROM manufacturing_iq.gold.production_kpis\n"
                "WHERE snapshot_time >= current_date()\n"
                "GROUP BY site ORDER BY avg_cpk ASC"
            ),
            "data": [
                {"site": "Detroit", "inspections": 980000, "oos_count": 6, "avg_cpk": 1.31},
                {"site": "Berlin", "inspections": 1200000, "oos_count": 4, "avg_cpk": 1.42},
                {"site": "Tokyo", "inspections": 674000, "oos_count": 4, "avg_cpk": 1.44},
            ],
        },
        {
            "patterns": [r"build", r"batch", r"track", r"production", r"yield", r"throughput"],
            "answer": (
                "**Build Tracking Summary**\n\n"
                "Active batches: **3** across all sites\n"
                "- **B-9982-XYZ** (Berlin): Complete -- 6 stations passed, 0 defects\n"
                "- **A-1102-MDF** (Detroit): **DEFECT** detected at station 3 -- vibration anomaly flagged\n"
                "- **C-4421-ALP** (Tokyo): In Progress -- currently at Assembly Line B (station 4/6)\n\n"
                "Overall yield rate: **98.0%** | OEE: **91.4%**\n"
                "Total units produced (24h): **148,320** with **2,966** defects.\n\n"
                "**Note:** Batch A-1102-MDF defect correlates with the CNC-Milling-BER-04 anomaly pattern. "
                "Recommend halting similar batches on Detroit line until tool is replaced."
            ),
            "sql": (
                "SELECT batch_id, site, station, status, defect_flag,\n"
                "       timestamp\n"
                "FROM manufacturing_iq.silver.build_tracking\n"
                "WHERE date(timestamp) = current_date()\n"
                "ORDER BY timestamp DESC"
            ),
            "data": [
                {"batch_id": "B-9982-XYZ", "site": "Berlin", "stations_complete": 6, "defects": 0, "status": "Complete"},
                {"batch_id": "A-1102-MDF", "site": "Detroit", "stations_complete": 3, "defects": 1, "status": "Defect"},
                {"batch_id": "C-4421-ALP", "site": "Tokyo", "stations_complete": 4, "defects": 0, "status": "In Progress"},
            ],
        },
        {
            "patterns": [r"model", r"f1", r"drift", r"ml", r"inference", r"latency", r"accuracy"],
            "answer": (
                "**ML Model Health Report**\n\n"
                "**CNC_Tolerance_Anomaly model:**\n"
                "- F1 Score: **0.947** (target: 0.94) -- Healthy\n"
                "- Inference latency: **42 ms** (p99: 68ms)\n"
                "- Data drift: **1.2%** (threshold: 5%) -- No significant drift\n"
                "- Predictions served (24h): **487,200**\n\n"
                "**Feature importance (SHAP):**\n"
                "1. vibration_hz: 0.34\n"
                "2. tool_wear_index: 0.28\n"
                "3. temp_c: 0.19\n"
                "4. spindle_rpm: 0.12\n"
                "5. feed_rate: 0.07\n\n"
                "**Inventory_Demand_Forecast model (Prophet):**\n"
                "- MAPE: 4.2% | Horizon: 30 days\n"
                "- Next retrain scheduled in 3 days."
            ),
            "sql": (
                "SELECT model_name, f1_score, inference_latency_ms,\n"
                "       data_drift_pct, predictions_24h\n"
                "FROM manufacturing_iq.gold.model_health_metrics\n"
                "ORDER BY snapshot_time DESC LIMIT 1"
            ),
            "data": [
                {"model_name": "CNC_Tolerance_Anomaly", "f1_score": 0.947, "inference_latency_ms": 42, "data_drift_pct": 1.2, "predictions_24h": 487200},
            ],
        },
        {
            "patterns": [r"q3", r"ramp", r"target", r"alpha", r"on track"],
            "answer": (
                "**Q3 Ramp Target Analysis**\n\n"
                "The Alpha-9 build is currently **on track** based on current production velocity.\n\n"
                "- Current daily output: **148,320 units** (target: 145,000)\n"
                "- Yield rate: **98.0%** (target: 97.5%)\n"
                "- Berlin is leading production at 52,200 units/day\n"
                "- Detroit is slightly behind target (-3.2%) due to the CNC line defects\n"
                "- Tokyo is at target with 44,800 units/day\n\n"
                "**Risk factor:** If the Detroit CNC anomaly is not resolved within 48 hours, "
                "the Q3 target is at risk of a **2-day delay**. Recommend prioritizing tool replacement."
            ),
            "sql": (
                "SELECT site, SUM(units_produced) AS daily_output,\n"
                "       AVG(yield_pct) AS avg_yield\n"
                "FROM manufacturing_iq.gold.production_kpis\n"
                "WHERE snapshot_time >= current_date()\n"
                "GROUP BY site"
            ),
            "data": [
                {"site": "Berlin", "daily_output": 52200, "avg_yield": 98.4},
                {"site": "Detroit", "daily_output": 51320, "avg_yield": 97.1},
                {"site": "Tokyo", "daily_output": 44800, "avg_yield": 98.5},
            ],
        },
        {
            "patterns": [r"downtime", r"berlin", r"cnc", r"root cause", r"why"],
            "answer": (
                "**Root Cause Analysis: Berlin CNC Milling Line Downtime**\n\n"
                "The primary cause of the current downtime on CNC-Milling-BER-04 is **excessive tool wear**.\n\n"
                "Evidence chain:\n"
                "1. Tool wear index reached **0.87** (replacement threshold: 0.80)\n"
                "2. This caused vibration to spike to **38.2 Hz** (2.5x normal)\n"
                "3. Temperature elevated to **72.4C** due to friction\n"
                "4. Anomaly model flagged this with score **0.94** at 14:23 UTC\n"
                "5. Automatic safety stop triggered at 14:25 UTC\n\n"
                "**Historical pattern:** This machine has had 3 similar events in the last 90 days, "
                "each correlated with tool wear exceeding 0.80. "
                "Current maintenance schedule has tool changes every 500 hours; "
                "recommend reducing to **400 hours** based on the data."
            ),
            "sql": (
                "SELECT timestamp, machine_id, vibration_hz, temp_c,\n"
                "       tool_wear_index, anomaly_score, event_type\n"
                "FROM manufacturing_iq.silver.cnc_anomalies\n"
                "WHERE machine_id = 'CNC-Milling-BER-04'\n"
                "  AND timestamp >= current_timestamp() - INTERVAL 24 HOURS\n"
                "ORDER BY timestamp DESC"
            ),
            "data": None,
        },
        {
            "patterns": [r"station", r"micro.?stop", r"stoppage", r"recurring"],
            "answer": (
                "**Top 5 Stations with Recurring Micro-Stoppages (Last 7 Days)**\n\n"
                "| Rank | Station | Site | Stoppages | Avg Duration | Impact |\n"
                "|------|---------|------|-----------|-------------|--------|\n"
                "| 1 | CNC Milling #4 | Berlin | 47 | 3.2 min | 2.5 hrs lost |\n"
                "| 2 | Lathe Line #12 | Detroit | 38 | 4.1 min | 2.6 hrs lost |\n"
                "| 3 | Assembly Arm #7 | Berlin | 31 | 2.8 min | 1.4 hrs lost |\n"
                "| 4 | Press #03 | Tokyo | 24 | 1.9 min | 0.8 hrs lost |\n"
                "| 5 | Assembly Arm #01 | Tokyo | 19 | 2.4 min | 0.8 hrs lost |\n\n"
                "Berlin CNC Milling #4 leads with 47 micro-stoppages, directly linked "
                "to the vibration anomaly. Lathe-DET-12 shows a distinct pattern of "
                "stoppages during shift changes."
            ),
            "sql": (
                "SELECT station_id, site, COUNT(*) AS stoppage_count,\n"
                "       AVG(duration_minutes) AS avg_duration_min\n"
                "FROM manufacturing_iq.silver.build_tracking\n"
                "WHERE event_type = 'MICRO_STOPPAGE'\n"
                "  AND timestamp >= current_date() - INTERVAL 7 DAYS\n"
                "GROUP BY station_id, site\n"
                "ORDER BY stoppage_count DESC LIMIT 5"
            ),
            "data": [
                {"station": "CNC Milling #4", "site": "Berlin", "stoppages": 47, "avg_duration_min": 3.2},
                {"station": "Lathe Line #12", "site": "Detroit", "stoppages": 38, "avg_duration_min": 4.1},
                {"station": "Assembly Arm #7", "site": "Berlin", "stoppages": 31, "avg_duration_min": 2.8},
            ],
        },
        {
            "patterns": [r"delivery", r"pipeline", r"align", r"schedule"],
            "answer": (
                "**Inventory vs. Delivery Pipeline Alignment**\n\n"
                "Overall alignment score: **78%** (target: 90%)\n\n"
                "Gaps identified:\n"
                "- **Sensor Array X-12**: Next delivery in 5 days, but stock runs out in 0.5 days. "
                "Gap of **4.5 days**. Expedite required.\n"
                "- **Precision Bearings**: Next delivery in 4 days, stock lasts 3 days. "
                "Minor gap -- can mitigate with reduced production rate.\n"
                "- **Ceramic Insulator Plate**: Next delivery in 3 days, stock lasts 2 days. "
                "Gap of 1 day. Safety stock should be increased.\n\n"
                "Well-aligned components: CNC Alloy Block A (14 days buffer), "
                "Titanium Fastener Set (21 days), Hydraulic Actuator B7 (7 days)."
            ),
            "sql": (
                "SELECT c.component, c.current_stock, c.daily_usage,\n"
                "       c.current_stock/c.daily_usage AS days_remaining,\n"
                "       f.next_delivery_date,\n"
                "       DATEDIFF(f.next_delivery_date, current_date()) AS delivery_in_days\n"
                "FROM manufacturing_iq.gold.site_component_status c\n"
                "JOIN manufacturing_iq.gold.inventory_forecast f ON c.component = f.component\n"
                "ORDER BY days_remaining ASC"
            ),
            "data": None,
        },
        {
            "patterns": [r"productivity", r"yield", r"rate", r"24.?hour", r"last.*hour", r"today"],
            "answer": (
                "**Production Productivity & Yield (Last 24 Hours)**\n\n"
                "Total units produced: **148,320**\n"
                "Overall yield: **98.0%** | Defect rate: **2.0%**\n"
                "OEE (Overall Equipment Effectiveness): **91.4%**\n\n"
                "By site:\n"
                "- Berlin: 52,200 units | 98.4% yield | OEE 93.1%\n"
                "- Detroit: 51,320 units | 97.1% yield | OEE 88.7%\n"
                "- Tokyo: 44,800 units | 98.5% yield | OEE 92.4%\n\n"
                "Detroit's lower OEE is driven by the CNC-Milling line downtime (32 minutes unplanned). "
                "Berlin and Tokyo are performing above target."
            ),
            "sql": (
                "SELECT site, total_units_produced, yield_pct, oee_pct\n"
                "FROM manufacturing_iq.gold.production_kpis\n"
                "WHERE snapshot_time >= current_timestamp() - INTERVAL 24 HOURS\n"
                "ORDER BY site"
            ),
            "data": [
                {"site": "Berlin", "units": 52200, "yield_pct": 98.4, "oee_pct": 93.1},
                {"site": "Detroit", "units": 51320, "yield_pct": 97.1, "oee_pct": 88.7},
                {"site": "Tokyo", "units": 44800, "yield_pct": 98.5, "oee_pct": 92.4},
            ],
        },
    ],

    # -----------------------------------------------------------------
    # RISK
    # -----------------------------------------------------------------
    "risk": [
        {
            "patterns": [r"compliance", r"posture", r"healthy", r"risk"],
            "answer": (
                "**Compliance Posture Overview**\n\n"
                "Overall compliance score: **84/100** (+3.1 from last month)\n\n"
                "Framework status:\n"
                "- **GDPR (EU):** Compliant -- 0 violations, last audit Oct 2025\n"
                "- **SOC 2 Type II (Global):** Compliant -- 0 violations\n"
                "- **CCPA (California):** Needs Review -- **2 violations** (data retention policy gaps)\n"
                "- **PCI-DSS (Global):** Needs Review -- **1 violation** (encryption key rotation overdue)\n"
                "- **HIPAA (US):** **At Risk** -- **14 violations** (unmasked PHI in Silver tables)\n\n"
                "**Highest risk area:** HIPAA compliance is the most critical concern. "
                "14 violations were detected in the last scan, primarily related to unmasked "
                "patient health information found in `risk_iq.silver.rbac_access_logs`. "
                "Immediate remediation is recommended."
            ),
            "sql": (
                "SELECT framework, status, violation_count, last_audit_date,\n"
                "       risk_score\n"
                "FROM risk_iq.gold.compliance_scores\n"
                "ORDER BY risk_score DESC"
            ),
            "data": [
                {"framework": "HIPAA", "status": "At Risk", "violations": 14, "risk_score": 92},
                {"framework": "CCPA", "status": "Needs Review", "violations": 2, "risk_score": 45},
                {"framework": "PCI-DSS", "status": "Needs Review", "violations": 1, "risk_score": 38},
                {"framework": "GDPR", "status": "Compliant", "violations": 0, "risk_score": 8},
                {"framework": "SOC 2", "status": "Compliant", "violations": 0, "risk_score": 5},
            ],
        },
        {
            "patterns": [r"pii", r"exposure", r"root cause", r"personal"],
            "answer": (
                "**PII Exposure Root Cause Analysis**\n\n"
                "In the last 24 hours, **142 PII anomalies** were flagged across **1.2 billion** scanned records.\n\n"
                "Breakdown by PII type:\n"
                "- PHONE_NUMBER: 42 detections\n"
                "- EMAIL_ADDRESS: 38 detections\n"
                "- US_SSN: 31 detections (CRITICAL)\n"
                "- CREDIT_CARD: 31 detections (CRITICAL)\n\n"
                "**Root cause:** A recent ETL pipeline update on March 5th bypassed the PII masking "
                "step in the Silver layer transformation for the `customer_pii_table`. "
                "The data engineering team has been notified and a hotfix PR was submitted 2 hours ago.\n\n"
                "**Current remediation:**\n"
                "1. Temporary access restriction applied to affected tables\n"
                "2. Masking job manually triggered -- expected completion in 45 min\n"
                "3. Audit log generated for affected records"
            ),
            "sql": (
                "SELECT pii_type, COUNT(*) AS detection_count,\n"
                "       MIN(detected_at) AS first_seen,\n"
                "       MAX(detected_at) AS last_seen\n"
                "FROM risk_iq.gold.pii_scan_results\n"
                "WHERE detected_at >= current_timestamp() - INTERVAL 24 HOURS\n"
                "GROUP BY pii_type\n"
                "ORDER BY detection_count DESC"
            ),
            "data": [
                {"pii_type": "PHONE_NUMBER", "detections": 42},
                {"pii_type": "EMAIL_ADDRESS", "detections": 38},
                {"pii_type": "US_SSN", "detections": 31},
                {"pii_type": "CREDIT_CARD", "detections": 31},
            ],
        },
        {
            "patterns": [r"gdpr", r"warning", r"impact", r"who"],
            "answer": (
                "**GDPR Compliance Warning Impact Assessment**\n\n"
                "Currently, GDPR status is **Compliant** with 0 active violations. "
                "However, the CCPA and HIPAA findings have cross-regulatory implications:\n\n"
                "**Potentially impacted:**\n"
                "- 2,847 EU-resident customer records in the Silver layer\n"
                "- 3 data processing pipelines that handle cross-border data\n"
                "- The Finance and Healthcare domains are most exposed\n\n"
                "**Key personnel:**\n"
                "- DPO notification sent to compliance@company.com\n"
                "- Data Engineering lead (j.doe@company.com) assigned to remediation\n"
                "- Legal review initiated for cross-border transfer assessment\n\n"
                "**Proactive measures:** The PII exposure in Silver tables could trigger GDPR "
                "Article 33 notification requirements if EU data is confirmed affected. "
                "Recommend completing the audit within 72 hours."
            ),
            "sql": (
                "SELECT domain, COUNT(DISTINCT record_id) AS affected_records,\n"
                "       COUNT(DISTINCT pipeline_id) AS affected_pipelines\n"
                "FROM risk_iq.silver.anomaly_detections\n"
                "WHERE region = 'EU'\n"
                "  AND detection_type = 'PII_EXPOSURE'\n"
                "GROUP BY domain"
            ),
            "data": None,
        },
        {
            "patterns": [r"rbac", r"logging", r"improve", r"future", r"access control"],
            "answer": (
                "**RBAC Logging Improvement Recommendations**\n\n"
                "Current RBAC logging captures 4 users with basic access tracking. "
                "Analysis of the access patterns reveals several gaps:\n\n"
                "**Recommended improvements:**\n"
                "1. **Granular action logging:** Currently tracking SELECT/EXPORT/VIEW/DOWNLOAD. "
                "Add UPDATE, DELETE, GRANT, REVOKE, and SCHEMA_CHANGE events.\n"
                "2. **Context enrichment:** Add source IP, session duration, and query hash "
                "to each log entry for forensic analysis.\n"
                "3. **Real-time alerting:** Deploy streaming-based anomaly detection on access logs "
                "using the IsolationForest model (already trained on features: access_frequency, "
                "data_sensitivity, time_of_day, role_match_score, geo_distance).\n"
                "4. **Retention policy:** Extend log retention from 90 to 365 days for SOC 2 compliance.\n"
                "5. **Unity Catalog integration:** Enable system tables for automatic audit logging.\n\n"
                "Estimated implementation effort: 3-4 sprints with the data engineering team."
            ),
            "sql": None,
            "data": None,
        },
        {
            "patterns": [r"anomalous", r"user", r"top.*five", r"top.*5", r"suspicious.*access"],
            "answer": (
                "**Top 5 Users with Anomalous Data Access**\n\n"
                "Based on the IsolationForest anomaly model scoring access patterns:\n\n"
                "| Rank | User | Anomaly Score | Flag | Details |\n"
                "|------|------|--------------|------|---------|\n"
                "| 1 | j.doe@company.com | 0.94 | Unusual Access Vector | Accessed payroll data from new IP at 2:30 AM |\n"
                "| 2 | a.smith@company.com | 0.87 | Under Review | Downloaded 14K records from customer_pii_table |\n"
                "| 3 | ml_service_account | 0.72 | Flagged | Elevated query frequency (4x baseline) |\n"
                "| 4 | sys_pipeline_01 | 0.58 | Standard | Accessed compliance_docs outside maintenance window |\n"
                "| 5 | b.jones@company.com | 0.54 | Standard | Cross-domain access (HR -> Finance) |\n\n"
                "j.doe@company.com is the highest priority -- the combination of off-hours access, "
                "new IP address, and sensitive payroll data access is highly unusual."
            ),
            "sql": (
                "SELECT user_identity, anomaly_score, risk_level,\n"
                "       asset_accessed, access_timestamp, source_ip\n"
                "FROM risk_iq.silver.rbac_access_logs\n"
                "WHERE anomaly_score > 0.5\n"
                "ORDER BY anomaly_score DESC LIMIT 5"
            ),
            "data": [
                {"user": "j.doe@company.com", "anomaly_score": 0.94, "flag": "Unusual Access Vector"},
                {"user": "a.smith@company.com", "anomaly_score": 0.87, "flag": "Under Review"},
                {"user": "ml_service_account", "anomaly_score": 0.72, "flag": "Flagged"},
                {"user": "sys_pipeline_01", "anomaly_score": 0.58, "flag": "Standard"},
                {"user": "b.jones@company.com", "anomaly_score": 0.54, "flag": "Standard"},
            ],
        },
        {
            "patterns": [r"data domain", r"regulatory", r"most risk"],
            "answer": (
                "**Regulatory Risk by Data Domain**\n\n"
                "| Domain | Risk Score | Open Violations | Frameworks Affected |\n"
                "|--------|-----------|----------------|--------------------|\n"
                "| Healthcare | 92 | 14 | HIPAA |\n"
                "| Customer | 48 | 2 | CCPA, GDPR |\n"
                "| Finance | 38 | 1 | PCI-DSS |\n"
                "| HR | 12 | 0 | SOC 2 |\n\n"
                "The **Healthcare domain** experiences the most regulatory risk by a significant margin, "
                "driven entirely by the HIPAA violations. The unmasked PHI in Silver tables "
                "represents a compliance risk score of 92/100.\n\n"
                "**Recommendation:** Focus remediation efforts on Healthcare domain first. "
                "The Customer domain's CCPA issues should be addressed within 30 days."
            ),
            "sql": (
                "SELECT domain, risk_score, open_violations,\n"
                "       frameworks_affected\n"
                "FROM risk_iq.gold.risk_summary\n"
                "ORDER BY risk_score DESC"
            ),
            "data": [
                {"domain": "Healthcare", "risk_score": 92, "violations": 14, "frameworks": "HIPAA"},
                {"domain": "Customer", "risk_score": 48, "violations": 2, "frameworks": "CCPA, GDPR"},
                {"domain": "Finance", "risk_score": 38, "violations": 1, "frameworks": "PCI-DSS"},
                {"domain": "HR", "risk_score": 12, "violations": 0, "frameworks": "SOC 2"},
            ],
        },
        {
            "patterns": [r"soc.*2", r"audit", r"fail"],
            "answer": (
                "**SOC 2 Audit Risk Assessment**\n\n"
                "Systems most likely to fail a SOC 2 Type II audit:\n\n"
                "1. **RBAC Logging System** (Risk: HIGH)\n"
                "   - Log retention only 90 days (SOC 2 requires 365)\n"
                "   - Missing granular action tracking for DELETE/UPDATE\n"
                "   - No automated alerting on policy violations\n\n"
                "2. **PII Data Pipeline** (Risk: HIGH)\n"
                "   - Recent masking bypass incident\n"
                "   - Insufficient access controls on Silver tables\n\n"
                "3. **ML Model Serving** (Risk: MEDIUM)\n"
                "   - Model governance documentation incomplete\n"
                "   - Input/output logging not enabled for all endpoints\n\n"
                "4. **Customer Data Lake** (Risk: LOW)\n"
                "   - Minor encryption key rotation delay (3 days overdue)\n\n"
                "Overall SOC 2 readiness: **72%**. Recommend addressing RBAC and PII items "
                "before the next audit cycle."
            ),
            "sql": None,
            "data": None,
        },
        {
            "patterns": [r"financial", r"exposure", r"risk score", r"total"],
            "answer": (
                "**Financial Risk Exposure Summary**\n\n"
                "Total financial risk exposure: **$2.4M** (+12% from last month)\n"
                "Compliance risk score: **84/100** (+3.1)\n"
                "Active high-severity alerts: **3** (-2 from last month)\n\n"
                "The increase in financial risk exposure is primarily driven by:\n"
                "1. HIPAA violation remediation costs (estimated $1.2M)\n"
                "2. Potential CCPA penalties ($450K)\n"
                "3. PCI-DSS remediation and audit costs ($750K)\n\n"
                "The reduction in high-severity alerts from 5 to 3 shows improvement in "
                "operational risk management. GDPR and SOC 2 frameworks are well-controlled."
            ),
            "sql": (
                "SELECT financial_risk_exposure, compliance_risk_score,\n"
                "       active_high_severity_alerts\n"
                "FROM risk_iq.gold.risk_summary\n"
                "ORDER BY snapshot_time DESC LIMIT 1"
            ),
            "data": None,
        },
    ],

    # -----------------------------------------------------------------
    # HEALTHCARE
    # -----------------------------------------------------------------
    "healthcare": [
        {
            "patterns": [r"bed", r"utiliz", r"capacity", r"occupancy"],
            "answer": (
                "**Bed Utilization Report**\n\n"
                "Current overall bed utilization: **87.3%**\n\n"
                "By facility:\n"
                "- **Metro General Hospital:** 91.2% -- NEAR CAPACITY\n"
                "  - 142 admissions today, ICU at 96% capacity\n"
                "- **Westside Medical Center:** 84.1% -- Normal\n"
                "  - 98 admissions today, trending stable\n"
                "- **Eastview Community Clinic:** 72.5% -- Normal\n"
                "  - 45 admissions today, good capacity buffer\n\n"
                "**Alert:** Metro General is projected to reach 95% capacity within 72 hours "
                "based on current admission trends. Recommend activating surge protocols "
                "and diverting non-critical cases to Eastview."
            ),
            "sql": (
                "SELECT facility, bed_utilization_pct, admissions_today,\n"
                "       icu_utilization_pct\n"
                "FROM healthcare_iq.gold.patient_flow_metrics\n"
                "WHERE snapshot_time >= current_date()\n"
                "ORDER BY bed_utilization_pct DESC"
            ),
            "data": [
                {"facility": "Metro General Hospital", "bed_util_pct": 91.2, "admissions": 142, "status": "Near Capacity"},
                {"facility": "Westside Medical Center", "bed_util_pct": 84.1, "admissions": 98, "status": "Normal"},
                {"facility": "Eastview Community Clinic", "bed_util_pct": 72.5, "admissions": 45, "status": "Normal"},
            ],
        },
        {
            "patterns": [r"wait", r"time", r"department", r"longest", r"ed", r"emergency"],
            "answer": (
                "**ED Wait Time Analysis**\n\n"
                "Average ED wait time across all facilities: **34 minutes**\n\n"
                "By department (longest first):\n"
                "| Department | Avg Wait | Volume | Trend |\n"
                "|-----------|----------|--------|-------|\n"
                "| Orthopedics | 52 min | 28 patients | Up 15% |\n"
                "| Cardiology | 45 min | 34 patients | Stable |\n"
                "| Emergency (General) | 38 min | 89 patients | Up 8% |\n"
                "| Oncology | 31 min | 12 patients | Down 5% |\n"
                "| Pediatrics | 24 min | 41 patients | Stable |\n"
                "| ICU | 18 min | 7 patients | Down 12% |\n\n"
                "**Orthopedics** has the longest wait at Metro General, driven by a spike in "
                "sports injury cases this week. Recommend adding a triage nurse during peak hours (10AM-2PM)."
            ),
            "sql": (
                "SELECT department, AVG(wait_time_minutes) AS avg_wait,\n"
                "       COUNT(*) AS patient_volume\n"
                "FROM healthcare_iq.silver.ed_encounters\n"
                "WHERE encounter_date = current_date()\n"
                "GROUP BY department\n"
                "ORDER BY avg_wait DESC"
            ),
            "data": [
                {"department": "Orthopedics", "avg_wait_min": 52, "patients": 28},
                {"department": "Cardiology", "avg_wait_min": 45, "patients": 34},
                {"department": "Emergency", "avg_wait_min": 38, "patients": 89},
                {"department": "Oncology", "avg_wait_min": 31, "patients": 12},
                {"department": "Pediatrics", "avg_wait_min": 24, "patients": 41},
            ],
        },
        {
            "patterns": [r"readmis", r"predict", r"discharg", r"48.?hour", r"risk"],
            "answer": (
                "**Readmission Risk -- Patients Discharged in Last 48 Hours**\n\n"
                "Model: Readmission_Risk_Predictor (AUC-ROC: 0.89)\n"
                "Discharges analyzed: **127 patients**\n\n"
                "Risk distribution:\n"
                "- High Risk (>70%): **18 patients** -- Immediate follow-up required\n"
                "- Medium Risk (40-70%): **34 patients** -- Schedule 7-day check-in\n"
                "- Low Risk (<40%): **75 patients** -- Standard discharge protocol\n\n"
                "Top high-risk patients:\n"
                "- PAT-000142: Heart Failure, 82 yr, 3 prior admissions -- Risk: 89%\n"
                "- PAT-000287: COPD, 71 yr, comorbidity index 4.2 -- Risk: 84%\n"
                "- PAT-000391: Pneumonia, 68 yr, recent ICU stay -- Risk: 78%\n\n"
                "Key risk factors: Prior admissions (12mo), comorbidity index, and length of stay "
                "are the top 3 predictors driving these scores."
            ),
            "sql": (
                "SELECT patient_id, diagnosis, age, risk_score,\n"
                "       prior_admissions_12m, comorbidity_index\n"
                "FROM healthcare_iq.gold.readmission_predictions\n"
                "WHERE discharge_date >= current_timestamp() - INTERVAL 48 HOURS\n"
                "  AND risk_score > 0.70\n"
                "ORDER BY risk_score DESC"
            ),
            "data": [
                {"patient_id": "PAT-000142", "diagnosis": "Heart Failure", "age": 82, "risk_score": 0.89},
                {"patient_id": "PAT-000287", "diagnosis": "COPD", "age": 71, "risk_score": 0.84},
                {"patient_id": "PAT-000391", "diagnosis": "Pneumonia", "age": 68, "risk_score": 0.78},
            ],
        },
        {
            "patterns": [r"equipment", r"maintenance", r"critical", r"alert"],
            "answer": (
                "**Critical Equipment Maintenance Alerts**\n\n"
                "Total assets monitored: **1,240**\n"
                "Maintenance due: **47** | Critical alerts: **3**\n\n"
                "Critical alerts requiring immediate action:\n"
                "1. **Ventilator-MGH-14** (Metro General) -- Motor bearing failure predicted in 12 hours. "
                "Backup unit assigned.\n"
                "2. **CT-Scanner-MGH-02** (Metro General) -- X-ray tube at 94% lifecycle. "
                "Replacement ordered, ETA 2 days.\n"
                "3. **Lab-Analyzer-EVC-08** (Eastview) -- Calibration drift detected. "
                "Results quarantined pending recalibration.\n\n"
                "**Recommendation:** Ventilator-MGH-14 is the highest priority given patient safety impact. "
                "Ensure backup is functional before scheduled maintenance window tonight."
            ),
            "sql": (
                "SELECT equipment_id, facility, alert_type, severity,\n"
                "       predicted_failure_hours, status\n"
                "FROM healthcare_iq.gold.equipment_health\n"
                "WHERE severity = 'Critical'\n"
                "ORDER BY predicted_failure_hours ASC"
            ),
            "data": [
                {"equipment_id": "Ventilator-MGH-14", "facility": "Metro General", "severity": "Critical", "failure_in_hours": 12},
                {"equipment_id": "CT-Scanner-MGH-02", "facility": "Metro General", "severity": "Critical", "failure_in_hours": 48},
                {"equipment_id": "Lab-Analyzer-EVC-08", "facility": "Eastview", "severity": "Critical", "failure_in_hours": 72},
            ],
        },
        {
            "patterns": [r"bottleneck", r"flow", r"patient flow"],
            "answer": (
                "**Patient Flow Bottleneck Analysis -- Emergency Department (This Week)**\n\n"
                "Key bottlenecks identified:\n\n"
                "1. **Triage to Bed Assignment:** Average 18 minutes (target: 10 min)\n"
                "   - Root cause: Bed management system update delays at Metro General\n"
                "2. **Lab Results Turnaround:** Average 62 minutes (target: 45 min)\n"
                "   - Root cause: Lab-Analyzer-EVC-08 calibration issues reducing throughput\n"
                "3. **Discharge Processing:** Average 2.4 hours (target: 1.5 hours)\n"
                "   - Root cause: Pharmacy verification backlog during PM shift\n\n"
                "Combined impact: These bottlenecks add ~1.5 hours to average patient journey, "
                "affecting approximately 89 ED patients per day. Estimated cost: $34K/day in "
                "extended stay charges."
            ),
            "sql": (
                "SELECT stage, avg_duration_minutes, target_minutes,\n"
                "       (avg_duration_minutes - target_minutes) AS delay_minutes\n"
                "FROM healthcare_iq.gold.patient_flow_metrics\n"
                "WHERE department = 'Emergency'\n"
                "  AND snapshot_date >= current_date() - INTERVAL 7 DAYS\n"
                "ORDER BY delay_minutes DESC"
            ),
            "data": None,
        },
        {
            "patterns": [r"facility", r"trend", r"capacity.*limit", r"72.*hour", r"project"],
            "answer": (
                "**Capacity Trend Forecast (Next 72 Hours)**\n\n"
                "Based on admission trends and scheduled discharges:\n\n"
                "- **Metro General Hospital:** Currently at 91.2%. "
                "Projected to reach **95.8%** in 48 hours. SURGE RISK.\n"
                "  - Admissions trending up 8% this week\n"
                "  - 12 scheduled surgeries will require post-op beds\n"
                "- **Westside Medical Center:** Stable at 84.1%. "
                "Projected: **82.5%** (slight improvement from planned discharges)\n"
                "- **Eastview Community Clinic:** Stable at 72.5%. "
                "Projected: **74.2%** (minor increase)\n\n"
                "**Action plan:** Begin diverting Metro General non-emergency admissions "
                "to Westside and Eastview starting tomorrow morning."
            ),
            "sql": None,
            "data": None,
        },
        {
            "patterns": [r"cardiology", r"predictor", r"30.?day", r"readmission.*factor"],
            "answer": (
                "**Top Readmission Predictors -- Cardiology (30-Day)**\n\n"
                "Based on SHAP analysis of the Readmission_Risk_Predictor model:\n\n"
                "| Rank | Feature | SHAP Value | Direction |\n"
                "|------|---------|-----------|----------|\n"
                "| 1 | prior_admissions_12m | 0.28 | Higher = More Risk |\n"
                "| 2 | comorbidity_index | 0.24 | Higher = More Risk |\n"
                "| 3 | los_days | 0.19 | Longer = More Risk |\n"
                "| 4 | age | 0.14 | Older = More Risk |\n"
                "| 5 | insurance_type | 0.09 | Medicare = Higher Risk |\n"
                "| 6 | diagnosis_code | 0.06 | Heart Failure = Highest |\n\n"
                "Key insight: Patients with 3+ prior admissions in 12 months AND comorbidity index >3.5 "
                "have an 82% readmission rate. This cohort represents 23% of cardiology discharges."
            ),
            "sql": (
                "SELECT feature, shap_importance, direction\n"
                "FROM healthcare_iq.gold.readmission_predictions\n"
                "WHERE department = 'Cardiology'\n"
                "ORDER BY shap_importance DESC"
            ),
            "data": None,
        },
    ],

    # -----------------------------------------------------------------
    # GAMING
    # -----------------------------------------------------------------
    "gaming": [
        {
            "patterns": [r"dau", r"daily active", r"trend", r"user.*this week"],
            "answer": (
                "**DAU Trend Analysis (This Week)**\n\n"
                "Current DAU: **2.4M** across all game titles\n\n"
                "By title:\n"
                "| Title | DAU | WoW Change | Peak Concurrent |\n"
                "|-------|-----|-----------|----------------|\n"
                "| Stellar Conquest | 1.1M | +5.2% | 412K |\n"
                "| Shadow Realms | 820K | -2.1% | 298K |\n"
                "| Velocity Rush | 480K | +12.8% | 137K |\n\n"
                "Velocity Rush is seeing the strongest growth, driven by the Season 3 launch "
                "last Tuesday. Shadow Realms DAU dip correlates with the server stability issues "
                "on NA-West region (resolved yesterday).\n\n"
                "Concurrent player peak: **847K** (hit at 21:00 UTC Sunday)"
            ),
            "sql": (
                "SELECT game_title, dau, wow_change_pct, concurrent_peak\n"
                "FROM gaming_iq.gold.live_ops_kpis\n"
                "WHERE snapshot_date >= current_date() - INTERVAL 7 DAYS\n"
                "ORDER BY dau DESC"
            ),
            "data": [
                {"title": "Stellar Conquest", "dau": "1.1M", "wow_change": "+5.2%", "concurrent": "412K"},
                {"title": "Shadow Realms", "dau": "820K", "wow_change": "-2.1%", "concurrent": "298K"},
                {"title": "Velocity Rush", "dau": "480K", "wow_change": "+12.8%", "concurrent": "137K"},
            ],
        },
        {
            "patterns": [r"churn", r"segment", r"stellar", r"risk"],
            "answer": (
                "**Churn Risk by Segment -- Stellar Conquest**\n\n"
                "Model: Player_Churn_Predictor (AUC-ROC: 0.91)\n\n"
                "| Segment | Players | High Churn Risk | D7 Retention | Avg LTV |\n"
                "|---------|---------|----------------|-------------|--------|\n"
                "| Whale | 12K | 840 (7%) | 64% | $284.50 |\n"
                "| Dolphin | 89K | 8,200 (9.2%) | 48% | $68.20 |\n"
                "| Minnow | 340K | 14,100 (4.1%) | 35% | $12.40 |\n"
                "| Free-to-Play | 660K | 11,060 (1.7%) | 28% | $2.10 |\n\n"
                "**Highest churn risk:** The **Dolphin** segment at 9.2% high churn risk. "
                "These are mid-spending players who haven't made a purchase in 14+ days. "
                "A targeted 20% discount offer could recover an estimated $560K in at-risk LTV.\n\n"
                "Top churn predictors: days_since_last_login (0.31), purchase_count_30d (0.24), "
                "session_frequency_7d (0.21)."
            ),
            "sql": (
                "SELECT segment, COUNT(*) AS players,\n"
                "       SUM(CASE WHEN churn_risk = 'High' THEN 1 ELSE 0 END) AS high_risk,\n"
                "       AVG(d7_retention) AS d7_retention\n"
                "FROM gaming_iq.gold.player_retention_cohorts\n"
                "WHERE game_title = 'Stellar Conquest'\n"
                "GROUP BY segment"
            ),
            "data": [
                {"segment": "Whale", "players": "12K", "high_risk": 840, "d7_retention": "64%"},
                {"segment": "Dolphin", "players": "89K", "high_risk": 8200, "d7_retention": "48%"},
                {"segment": "Minnow", "players": "340K", "high_risk": 14100, "d7_retention": "35%"},
                {"segment": "Free-to-Play", "players": "660K", "high_risk": 11060, "d7_retention": "28%"},
            ],
        },
        {
            "patterns": [r"gold", r"duplic", r"exploit", r"shadow"],
            "answer": (
                "**Exploit Investigation: Shadow Realms Gold Duplication**\n\n"
                "The Economy_Fraud_Detector (IsolationForest) has flagged **23 accounts** "
                "with suspicious gold generation patterns in Shadow Realms.\n\n"
                "Evidence:\n"
                "- 23 accounts generated 4.2M gold in 2 hours (normal rate: ~5K/hour)\n"
                "- All accounts created within the same 48-hour window\n"
                "- Common pattern: trade gold to mule accounts, then convert to rare items\n"
                "- Exploit vector: Crafting recipe bug allowing negative-cost input materials\n\n"
                "**Impact:** Estimated 2.1B illegitimate gold injected into economy. "
                "Inflation index spiked to 1.08 in affected market regions.\n\n"
                "**Actions taken:**\n"
                "1. 23 accounts suspended pending investigation\n"
                "2. Crafting recipe hotfix deployed at 04:00 UTC\n"
                "3. Economic rollback prepared for affected transactions\n"
                "4. 89 total suspicious transactions flagged for review"
            ),
            "sql": (
                "SELECT account_id, gold_generated_1h, transaction_velocity,\n"
                "       account_age_days, anomaly_score\n"
                "FROM gaming_iq.silver.transaction_log\n"
                "WHERE game_title = 'Shadow Realms'\n"
                "  AND anomaly_score > 0.85\n"
                "ORDER BY gold_generated_1h DESC"
            ),
            "data": None,
        },
        {
            "patterns": [r"retention", r"d1", r"d7", r"d30", r"cohort", r"latest"],
            "answer": (
                "**Retention Cohort Analysis**\n\n"
                "Latest cohort (installed 30+ days ago):\n"
                "- D1 Retention: **68%** (industry avg: 40%)\n"
                "- D7 Retention: **41%** (industry avg: 20%)\n"
                "- D30 Retention: **22%** (industry avg: 10%)\n\n"
                "By segment performance:\n"
                "| Segment | D1 | D7 | D30 | LTV |\n"
                "|---------|-----|-----|------|-----|\n"
                "| Whale | 82% | 64% | 45% | $284.50 |\n"
                "| Dolphin | 74% | 48% | 28% | $68.20 |\n"
                "| Minnow | 65% | 35% | 15% | $12.40 |\n"
                "| Free-to-Play | 58% | 28% | 8% | $2.10 |\n\n"
                "Our retention significantly exceeds industry benchmarks across all time horizons. "
                "The D30 cohort shows Velocity Rush's Season 3 update improved D7 retention by 4 "
                "percentage points compared to the previous cohort."
            ),
            "sql": (
                "SELECT segment, d1_retention, d7_retention, d30_retention, avg_ltv\n"
                "FROM gaming_iq.gold.player_retention_cohorts\n"
                "ORDER BY avg_ltv DESC"
            ),
            "data": [
                {"segment": "Whale", "d1": "82%", "d7": "64%", "d30": "45%", "ltv": "$284.50"},
                {"segment": "Dolphin", "d1": "74%", "d7": "48%", "d30": "28%", "ltv": "$68.20"},
                {"segment": "Minnow", "d1": "65%", "d7": "35%", "d30": "15%", "ltv": "$12.40"},
                {"segment": "Free-to-Play", "d1": "58%", "d7": "28%", "d30": "8%", "ltv": "$2.10"},
            ],
        },
        {
            "patterns": [r"matchmaking", r"fairness", r"velocity", r"ranked"],
            "answer": (
                "**Matchmaking Fairness -- Velocity Rush Ranked Mode**\n\n"
                "Average queue time: **12.4 seconds** (target: <15s)\n"
                "Skill rating spread: **0.15** (lower is fairer, target: <0.20)\n"
                "Matches in 24h: **4.2M**\n"
                "Reported unfair matches: **0.8%** (target: <1.0%)\n\n"
                "Regional breakdown:\n"
                "- NA-East: 8.2s queue, 0.92 fairness -- Best performing\n"
                "- EU-West: 14.1s queue, 0.91 fairness -- Slightly long queues\n"
                "- APAC-JP: 13.6s queue, 0.87 fairness -- Needs attention\n\n"
                "APAC-JP has the lowest fairness score due to smaller player pool during "
                "off-peak hours. Recommend enabling cross-region matching during 02:00-10:00 JST."
            ),
            "sql": (
                "SELECT region, avg_queue_time_sec, fairness_score,\n"
                "       matches_24h\n"
                "FROM gaming_iq.gold.matchmaking_fairness\n"
                "WHERE game_title = 'Velocity Rush'\n"
                "  AND mode = 'ranked'\n"
                "ORDER BY fairness_score ASC"
            ),
            "data": [
                {"region": "NA-East", "queue": "8.2s", "fairness": 0.92, "matches": "1.1M"},
                {"region": "EU-West", "queue": "14.1s", "fairness": 0.91, "matches": "720K"},
                {"region": "APAC-JP", "queue": "13.6s", "fairness": 0.87, "matches": "320K"},
            ],
        },
        {
            "patterns": [r"arpdau", r"region.*driv", r"revenue", r"highest"],
            "answer": (
                "**ARPDAU by Region**\n\n"
                "Overall ARPDAU: **$0.118**\n\n"
                "| Region | ARPDAU | DAU | Daily Revenue | Driver |\n"
                "|--------|--------|-----|-------------|--------|\n"
                "| APAC-JP | $0.182 | 380K | $69.2K | Gacha mechanics |\n"
                "| NA-East | $0.142 | 620K | $88.0K | Battle pass |\n"
                "| EU-West | $0.104 | 540K | $56.2K | Cosmetics |\n"
                "| NA-West | $0.098 | 420K | $41.2K | Battle pass |\n"
                "| EU-North | $0.081 | 280K | $22.7K | Cosmetics |\n"
                "| APAC-SEA | $0.042 | 160K | $6.7K | Limited purchasing |\n\n"
                "**APAC-JP** has the highest ARPDAU at $0.182, driven primarily by Stellar Conquest's "
                "gacha-style limited-time event shop. NA-East generates the highest absolute revenue "
                "due to larger player base."
            ),
            "sql": (
                "SELECT region, arpdau, dau, daily_revenue,\n"
                "       top_revenue_driver\n"
                "FROM gaming_iq.gold.economy_health_metrics\n"
                "ORDER BY arpdau DESC"
            ),
            "data": None,
        },
        {
            "patterns": [r"inflat", r"item", r"economy", r"top.*5", r"caus"],
            "answer": (
                "**Top 5 Items Causing Economy Inflation (This Week)**\n\n"
                "Current inflation index: **1.03** (target: <1.05)\n\n"
                "| Rank | Item | Game | Price Change | Volume | Impact |\n"
                "|------|------|------|-------------|--------|--------|\n"
                "| 1 | Dragon Scale Armor | Shadow Realms | +34% | 42K trades | High |\n"
                "| 2 | Mythic Engine Part | Velocity Rush | +28% | 18K trades | Medium |\n"
                "| 3 | Starship Blueprint | Stellar Conquest | +22% | 31K trades | Medium |\n"
                "| 4 | Enchanted Amulet | Shadow Realms | +19% | 55K trades | Medium |\n"
                "| 5 | Premium Fuel Cell | Velocity Rush | +15% | 24K trades | Low |\n\n"
                "Dragon Scale Armor's 34% price spike is linked to the gold duplication exploit -- "
                "exploiters were converting illicit gold into rare items. Price should normalize "
                "after the economic rollback."
            ),
            "sql": (
                "SELECT item_name, game_title, price_change_pct,\n"
                "       trade_volume_7d, inflation_impact\n"
                "FROM gaming_iq.gold.economy_health_metrics\n"
                "WHERE metric_type = 'item_inflation'\n"
                "ORDER BY price_change_pct DESC LIMIT 5"
            ),
            "data": None,
        },
    ],

    # -----------------------------------------------------------------
    # FINANCIAL SERVICES
    # -----------------------------------------------------------------
    "financial_services": [
        {
            "patterns": [r"fraud", r"detection", r"rate", r"compare", r"month"],
            "answer": (
                "**Fraud Detection Performance**\n\n"
                "Current fraud detection rate: **99.77%** (vs. 99.71% last month)\n"
                "Transactions today: **12.5M** | Fraud blocked: **847**\n"
                "False positive rate: **0.40%** (improved from 0.52%)\n"
                "Average fraud amount: **$3,420**\n\n"
                "Model: Transaction_Fraud_Detector (XGBClassifier, AUC-ROC: 0.97)\n\n"
                "Month-over-month improvement:\n"
                "- Detection rate: +0.06 pp\n"
                "- False positives: -23% reduction (saved ~$1.2M in manual review costs)\n"
                "- Average response time: 12ms (real-time blocking)\n\n"
                "The improvement is attributed to the model retrain on Feb 28 that incorporated "
                "new device fingerprinting features."
            ),
            "sql": (
                "SELECT snapshot_date, fraud_detection_rate, false_positive_rate,\n"
                "       total_blocked, avg_fraud_amount\n"
                "FROM financial_services_iq.gold.fraud_detection_metrics\n"
                "ORDER BY snapshot_date DESC LIMIT 30"
            ),
            "data": [
                {"metric": "Detection Rate", "current": "99.77%", "last_month": "99.71%", "change": "+0.06 pp"},
                {"metric": "False Positive Rate", "current": "0.40%", "last_month": "0.52%", "change": "-23%"},
                {"metric": "Fraud Blocked Today", "current": 847, "last_month": 812, "change": "+4.3%"},
            ],
        },
        {
            "patterns": [r"merchant", r"category", r"velocity", r"highest.*fraud"],
            "answer": (
                "**Fraud Velocity by Merchant Category**\n\n"
                "| Category | Fraud Rate | Blocked | Velocity (txn/min) | Risk |\n"
                "|----------|-----------|---------|-------------------|------|\n"
                "| Gift Cards & Prepaid | 0.0142% | 312 | 8.4 | Critical |\n"
                "| Wire Transfers | 0.0098% | 68 | 2.1 | High |\n"
                "| ATM Withdrawals | 0.0075% | 142 | 4.8 | High |\n"
                "| Mobile Payments | 0.0074% | 312 | 12.2 | Medium |\n"
                "| Online Retail | 0.0052% | 198 | 6.1 | Medium |\n"
                "| In-Branch | 0.0018% | 87 | 1.4 | Low |\n\n"
                "**Gift Cards & Prepaid** has the highest fraud rate at 0.0142%, "
                "driven by organized fraud rings purchasing high-value gift cards with stolen credentials. "
                "Recommend implementing mandatory 2FA for gift card purchases over $500."
            ),
            "sql": (
                "SELECT merchant_category, fraud_rate, blocked_count,\n"
                "       fraud_velocity_per_min, risk_level\n"
                "FROM financial_services_iq.gold.fraud_detection_metrics\n"
                "WHERE metric_type = 'by_category'\n"
                "ORDER BY fraud_rate DESC"
            ),
            "data": None,
        },
        {
            "patterns": [r"credit", r"default", r"distribution", r"commercial", r"lending"],
            "answer": (
                "**Credit Default Probability Distribution -- Commercial Lending**\n\n"
                "Portfolio value: **$3.4B** | Avg credit score: **698** | Delinquency rate: **4.1%**\n\n"
                "Default probability distribution:\n"
                "| PD Range | Accounts | Exposure | Expected Loss |\n"
                "|----------|----------|----------|---------------|\n"
                "| 0-1% (Prime) | 4,200 | $1.8B | $9.0M |\n"
                "| 1-5% (Standard) | 2,800 | $1.1B | $27.5M |\n"
                "| 5-15% (Subprime) | 680 | $380M | $28.5M |\n"
                "| >15% (High Risk) | 120 | $120M | $24.0M |\n\n"
                "Model: Credit_Default_Predictor (GradientBoosting, AUC-ROC: 0.88)\n\n"
                "The subprime and high-risk segments represent only 10% of accounts but "
                "59% of expected losses. Recommend tightening origination criteria for "
                "DTI ratios above 45%."
            ),
            "sql": (
                "SELECT pd_range, account_count, total_exposure,\n"
                "       expected_loss\n"
                "FROM financial_services_iq.gold.credit_risk_portfolio\n"
                "WHERE business_line = 'Commercial Lending'\n"
                "ORDER BY pd_range"
            ),
            "data": [
                {"pd_range": "0-1%", "accounts": 4200, "exposure": "$1.8B", "expected_loss": "$9.0M"},
                {"pd_range": "1-5%", "accounts": 2800, "exposure": "$1.1B", "expected_loss": "$27.5M"},
                {"pd_range": "5-15%", "accounts": 680, "exposure": "$380M", "expected_loss": "$28.5M"},
                {"pd_range": ">15%", "accounts": 120, "exposure": "$120M", "expected_loss": "$24.0M"},
            ],
        },
        {
            "patterns": [r"money laundering", r"aml", r"suspicious", r"pattern"],
            "answer": (
                "**Suspicious Transaction Patterns -- AML Screening**\n\n"
                "The fraud model has flagged **89 suspicious transaction patterns** in the last 24 hours "
                "that warrant AML investigation:\n\n"
                "Pattern types detected:\n"
                "1. **Structuring (smurfing):** 34 cases -- Multiple deposits just under $10K threshold\n"
                "2. **Rapid movement:** 28 cases -- Funds moved through 3+ accounts within 1 hour\n"
                "3. **Unusual geography:** 18 cases -- Transactions from sanctioned or high-risk jurisdictions\n"
                "4. **Dormant account activation:** 9 cases -- Previously inactive accounts suddenly active\n\n"
                "Priority investigations:\n"
                "- Account #TXN-8847: $487K moved through 5 accounts in 45 minutes\n"
                "- Account #TXN-9912: 12 deposits of $9,500 each over 3 days\n\n"
                "All flagged cases have been submitted to the compliance team via the SAR workflow."
            ),
            "sql": (
                "SELECT transaction_id, pattern_type, amount, account_count,\n"
                "       anomaly_score, flagged_at\n"
                "FROM financial_services_iq.silver.transaction_anomalies\n"
                "WHERE pattern_type IN ('structuring', 'rapid_movement', 'unusual_geography')\n"
                "  AND flagged_at >= current_timestamp() - INTERVAL 24 HOURS\n"
                "ORDER BY anomaly_score DESC"
            ),
            "data": None,
        },
        {
            "patterns": [r"var", r"value at risk", r"stress", r"market"],
            "answer": (
                "**Portfolio Value at Risk -- Stressed Conditions**\n\n"
                "AUM Total: **$24.5B** | Active Positions: **8,450**\n\n"
                "| Scenario | VaR (95%) | VaR (99%) | Expected Shortfall |\n"
                "|----------|----------|----------|-------------------|\n"
                "| Normal | $47M | $72M | $89M |\n"
                "| Moderate Stress | $94M | $144M | $178M |\n"
                "| Severe Stress (2008-like) | $235M | $361M | $445M |\n"
                "| Black Swan | $470M | $722M | $891M |\n\n"
                "Current portfolio metrics:\n"
                "- Sharpe Ratio: **1.42** | Beta: **0.94**\n"
                "- Under severe stress, the maximum drawdown is estimated at **1.47%** of AUM\n"
                "- Concentration risk: Top 5 positions represent 12% of AUM\n\n"
                "The portfolio is well-diversified with a beta below 1.0, indicating lower "
                "market sensitivity. Current VaR levels are within risk appetite."
            ),
            "sql": (
                "SELECT scenario, var_95, var_99, expected_shortfall\n"
                "FROM financial_services_iq.gold.portfolio_market_risk\n"
                "WHERE snapshot_date = current_date()"
            ),
            "data": None,
        },
        {
            "patterns": [r"delinquency", r"segment", r"customer", r"fastest.*growing"],
            "answer": (
                "**Delinquency Rate by Customer Segment**\n\n"
                "| Segment | 30d Delinquency | Trend (90d) | Accounts | Exposure |\n"
                "|---------|----------------|-------------|----------|----------|\n"
                "| Small Business | 5.8% | +1.2 pp | 3,400 | $890M |\n"
                "| Young Professionals | 4.2% | +0.8 pp | 8,900 | $1.2B |\n"
                "| Retail Banking | 3.2% | +0.3 pp | 42,000 | $4.8B |\n"
                "| Wealth Management | 0.8% | -0.1 pp | 2,100 | $6.2B |\n\n"
                "**Small Business** has the fastest growing delinquency rate (+1.2 pp over 90 days), "
                "driven by seasonal cash flow challenges and rising interest rates. "
                "Recommend proactive outreach to the 340 accounts showing early warning signs "
                "(1-15 days past due)."
            ),
            "sql": (
                "SELECT customer_segment, delinquency_rate_30d,\n"
                "       delinquency_trend_90d, account_count, total_exposure\n"
                "FROM financial_services_iq.gold.credit_risk_portfolio\n"
                "ORDER BY delinquency_trend_90d DESC"
            ),
            "data": [
                {"segment": "Small Business", "delinquency": "5.8%", "trend": "+1.2 pp", "accounts": 3400},
                {"segment": "Young Professionals", "delinquency": "4.2%", "trend": "+0.8 pp", "accounts": 8900},
                {"segment": "Retail Banking", "delinquency": "3.2%", "trend": "+0.3 pp", "accounts": 42000},
                {"segment": "Wealth Management", "delinquency": "0.8%", "trend": "-0.1 pp", "accounts": 2100},
            ],
        },
        {
            "patterns": [r"q2", r"expected.*credit.*loss", r"origination", r"predict"],
            "answer": (
                "**Expected Credit Loss Prediction -- Q2 Loan Originations**\n\n"
                "Based on the Credit_Default_Predictor model applied to the current pipeline:\n\n"
                "Projected Q2 originations: **$1.8B** across 4,200 new loans\n"
                "Expected loss rate: **1.8%** | Expected loss: **$32.4M**\n\n"
                "By business line:\n"
                "| Business Line | Originations | ECL Rate | ECL Amount |\n"
                "|-------------|-------------|---------|------------|\n"
                "| Retail Banking | $680M | 1.4% | $9.5M |\n"
                "| Commercial Lending | $520M | 2.8% | $14.6M |\n"
                "| Wealth Management | $380M | 0.6% | $2.3M |\n"
                "| Insurance | $220M | 2.7% | $5.9M |\n\n"
                "The Q2 ECL is 8% higher than Q1 due to the rising delinquency trend in "
                "Small Business and tightening credit conditions. Recommend increasing "
                "loan loss provisions by $2.6M."
            ),
            "sql": (
                "SELECT business_line, projected_originations,\n"
                "       ecl_rate, ecl_amount\n"
                "FROM financial_services_iq.silver.credit_default_predictions\n"
                "WHERE quarter = 'Q2-2026'\n"
                "ORDER BY ecl_amount DESC"
            ),
            "data": None,
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
        "manufacturing": "manufacturing operations",
        "risk": "risk and compliance",
        "healthcare": "clinical operations",
        "gaming": "player analytics and live ops",
        "financial_services": "financial risk and portfolio management",
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

    Tries backends in order:
      1. Databricks Genie API  (if GENIE_SPACE_ID is set)
      2. Databricks Foundation Model  (if DATABRICKS_FM_ENDPOINT is set)
      3. Demo mode  (always available)

    Parameters
    ----------
    question : str
        The user's natural-language question.
    use_case : str
        The vertical identifier (e.g. 'manufacturing', 'risk').

    Returns
    -------
    dict
        {"answer": str, "sql": str|None, "source": str, "data": list|None}
    """
    # Backend 1: Genie API
    if _get_genie_space_id():
        try:
            return _ask_genie_api(question, use_case)
        except Exception as e:
            logger.warning("Genie API failed, falling back: %s", e)

    # Backend 2: Foundation Model
    if _get_fm_endpoint():
        try:
            return _ask_fm(question, use_case)
        except Exception as e:
            logger.warning("FM endpoint failed, falling back: %s", e)

    # Backend 3: Demo mode (always available)
    return _ask_demo(question, use_case)
