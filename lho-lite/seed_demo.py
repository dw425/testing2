#!/usr/bin/env python3
"""Seed LHO Lite with realistic demo data for UI testing (no Databricks connection needed)."""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from app.config_store import save_config, save_data_snapshot

# --- Fake config so has_config() returns True ---
save_config({
    "workspace_url": "https://demo-workspace.cloud.databricks.com",
    "auth_method": "pat",
    "pat_token": "dapi_DEMO_TOKEN_NOT_REAL",
    "refresh_schedule": "manual",
})

# --- Realistic demo security data ---
sec_data = {
    "_cloud": "AWS",
    "_govcloud": False,
    "_workspace_url": "demo-workspace.cloud.databricks.com",
    "me": {"userName": "admin@blueprint.io", "displayName": "Admin User"},
    "users": {
        "totalResults": 47,
        "Resources": [
            {"userName": f"user{i}@blueprint.io", "displayName": f"User {i}", "active": True}
            for i in range(1, 40)
        ] + [
            {"userName": "contractor1@acme-consulting.com", "displayName": "Contractor 1", "active": True},
            {"userName": "contractor2@acme-consulting.com", "displayName": "Contractor 2", "active": True},
            {"userName": "vendor@external-vendor.io", "displayName": "Vendor User", "active": True},
            {"userName": "bob@gmail.com", "displayName": "Bob Personal", "active": True},
            {"userName": "alice@outlook.com", "displayName": "Alice Personal", "active": True},
            {"userName": "data-eng-sp@blueprint.io", "displayName": "Data Eng SP", "active": True},
            {"userName": "ml-ops-sp@blueprint.io", "displayName": "ML Ops SP", "active": True},
            {"userName": "admin@blueprint.io", "displayName": "Admin User", "active": True},
        ],
    },
    "groups": {
        "Resources": [
            {"displayName": "admins", "members": [{"value": f"u{i}"} for i in range(1, 6)]},
            {"displayName": "data-engineers", "members": [{"value": f"u{i}"} for i in range(6, 20)]},
            {"displayName": "data-scientists", "members": [{"value": f"u{i}"} for i in range(20, 35)]},
            {"displayName": "analysts", "members": [{"value": f"u{i}"} for i in range(35, 45)]},
            {"displayName": "mlops", "members": [{"value": f"u{i}"} for i in range(45, 48)]},
        ]
    },
    "sps": {
        "Resources": [
            {"applicationId": "sp-data-eng-001", "displayName": "data-eng-pipeline"},
            {"applicationId": "sp-ml-ops-002", "displayName": "ml-ops-scoring"},
            {"applicationId": "sp-etl-003", "displayName": "etl-scheduler"},
        ]
    },
    "clusters": {
        "clusters": [
            {"cluster_name": "shared-analytics", "cluster_id": "0301-prod-analytics", "state": "RUNNING",
             "spark_version": "14.3.x-scala2.12", "node_type_id": "i3.xlarge",
             "num_workers": 4, "autotermination_minutes": 30, "enable_local_disk_encryption": True,
             "data_security_mode": "USER_ISOLATION", "creator_user_name": "admin@blueprint.io"},
            {"cluster_name": "ml-training-gpu", "cluster_id": "0302-ml-gpu", "state": "TERMINATED",
             "spark_version": "14.3.x-gpu-ml-scala2.12", "node_type_id": "p3.2xlarge",
             "num_workers": 8, "autotermination_minutes": 60, "enable_local_disk_encryption": True,
             "data_security_mode": "USER_ISOLATION", "creator_user_name": "user20@blueprint.io"},
            {"cluster_name": "etl-batch-nightly", "cluster_id": "0303-etl-batch", "state": "TERMINATED",
             "spark_version": "14.3.x-scala2.12", "node_type_id": "m5.2xlarge",
             "num_workers": 12, "autotermination_minutes": 20, "enable_local_disk_encryption": True,
             "data_security_mode": "USER_ISOLATION", "creator_user_name": "data-eng-sp@blueprint.io"},
            {"cluster_name": "dev-sandbox", "cluster_id": "0304-dev-sandbox", "state": "RUNNING",
             "spark_version": "14.3.x-scala2.12", "node_type_id": "m5.xlarge",
             "num_workers": 2, "autotermination_minutes": 0, "enable_local_disk_encryption": False,
             "data_security_mode": "NONE", "creator_user_name": "user5@blueprint.io"},
            {"cluster_name": "streaming-ingest", "cluster_id": "0305-streaming", "state": "RUNNING",
             "spark_version": "14.3.x-scala2.12", "node_type_id": "r5.xlarge",
             "num_workers": 3, "autotermination_minutes": 0, "enable_local_disk_encryption": True,
             "data_security_mode": "USER_ISOLATION", "creator_user_name": "data-eng-sp@blueprint.io"},
        ]
    },
    "policies": {
        "policies": [
            {"name": "Standard Analytics", "policy_id": "pol-001",
             "definition": '{"data_security_mode": {"value": "USER_ISOLATION"}, "autotermination_minutes": {"value": "30"}}'},
            {"name": "ML Training", "policy_id": "pol-002",
             "definition": '{"data_security_mode": {"value": "USER_ISOLATION"}, "node_type_id": {"value": "p3.2xlarge"}}'},
            {"name": "Dev Sandbox (Legacy)", "policy_id": "pol-003",
             "definition": '{"data_security_mode": {"value": "NONE"}}'},
        ]
    },
    "warehouses": {
        "warehouses": [
            {"id": "wh-001", "name": "Reporting Warehouse", "state": "RUNNING", "cluster_size": "Medium",
             "num_clusters": 1, "auto_stop_mins": 15, "warehouse_type": "PRO",
             "creator_name": "admin@blueprint.io"},
            {"id": "wh-002", "name": "Ad-Hoc Queries", "state": "RUNNING", "cluster_size": "Small",
             "num_clusters": 1, "auto_stop_mins": 10, "warehouse_type": "PRO",
             "creator_name": "admin@blueprint.io"},
            {"id": "wh-003", "name": "ETL Processing", "state": "STOPPED", "cluster_size": "Large",
             "num_clusters": 2, "auto_stop_mins": 5, "warehouse_type": "PRO",
             "creator_name": "data-eng-sp@blueprint.io"},
        ]
    },
    "jobs": {
        "jobs": [
            {"job_id": j, "settings": {"name": name}}
            for j, name in enumerate([
                "nightly-etl-bronze", "nightly-etl-silver", "nightly-etl-gold",
                "ml-feature-pipeline", "ml-model-retrain-weekly", "ml-scoring-batch",
                "reporting-daily-refresh", "data-quality-checks", "audit-log-export",
                "catalog-sync", "cost-attribution", "sla-monitor",
            ], start=100)
        ]
    },
    "ip_lists": {"ip_access_lists": []},
    "secrets": {
        "scopes": [
            {"name": "production-keys", "backend_type": "DATABRICKS"},
        ]
    },
    "tokens": {
        "token_infos": [
            {"token_id": f"tok-{i}", "creation_time": 1700000000000 + i * 86400000,
             "expiry_time": 1700000000000 + (i + 180) * 86400000,
             "created_by_username": f"user{i}@blueprint.io"}
            for i in range(1, 12)
        ]
    },
    "init_scripts": {"scripts": [
        {"script_id": "is-001", "name": "setup-monitoring.sh"},
        {"script_id": "is-002", "name": "install-libs.sh"},
    ]},
    "init_contents": {},
    "workspace_conf": {
        "enableDbfsFileBrowser": "true",
        "maxTokenLifetimeDays": "180",
        "enableExportNotebook": "true",
        "enableResultsDownloading": "true",
        "enableWebTerminal": "false",
    },
    "metastores": {
        "metastores": [
            {"metastore_id": "ms-001", "name": "primary-metastore", "cloud": "AWS",
             "region": "us-east-1", "storage_root": "s3://databricks-metastore-prod",
             "created_at": 1680000000000, "owner": "admin@blueprint.io"},
        ]
    },
    "catalogs": {
        "catalogs": [
            {"name": "main", "catalog_type": "MANAGED_CATALOG", "owner": "admin@blueprint.io"},
            {"name": "raw_data", "catalog_type": "MANAGED_CATALOG", "owner": "data-eng-sp@blueprint.io"},
            {"name": "curated", "catalog_type": "MANAGED_CATALOG", "owner": "data-eng-sp@blueprint.io"},
            {"name": "ml_features", "catalog_type": "MANAGED_CATALOG", "owner": "user20@blueprint.io"},
            {"name": "sandbox", "catalog_type": "MANAGED_CATALOG", "owner": "admin@blueprint.io"},
        ]
    },
    "storage_creds": {
        "storage_credentials": [
            {"name": "s3-prod-access", "aws_iam_role": {"role_arn": "arn:aws:iam::123456789012:role/databricks-prod"}},
            {"name": "s3-staging-access", "aws_iam_role": {"role_arn": "arn:aws:iam::123456789012:role/databricks-staging"}},
        ]
    },
    "ext_locations": {
        "external_locations": [
            {"name": "prod-landing", "url": "s3://prod-landing-zone/", "credential_name": "s3-prod-access"},
            {"name": "staging-exports", "url": "s3://staging-exports/", "credential_name": "s3-staging-access"},
            {"name": "ml-artifacts", "url": "s3://ml-model-artifacts/", "credential_name": "s3-prod-access"},
        ]
    },
    "shares": {
        "shares": [
            {"name": "partner-data-share", "created_at": 1700000000000, "created_by": "admin@blueprint.io"},
        ]
    },
    "recipients": {
        "recipients": [
            {"name": "partner-acme", "authentication_type": "TOKEN"},
            {"name": "partner-globex", "authentication_type": "TOKEN"},
        ]
    },
    "apps": {
        "apps": [
            {"name": "LHO Lite", "description": "Lakehouse Optimizer", "url": "/apps/lho-lite",
             "create_time": "2024-01-15T10:00:00Z", "creator": "admin@blueprint.io",
             "status": {"state": "RUNNING"}},
            {"name": "Data Quality Monitor", "description": "DQ dashboard", "url": "/apps/dq-monitor",
             "create_time": "2024-02-01T08:00:00Z", "creator": "data-eng-sp@blueprint.io",
             "status": {"state": "RUNNING"}},
            {"name": "Feature Store UI", "description": "ML feature explorer", "url": "/apps/feature-store",
             "create_time": "2024-03-10T14:00:00Z", "creator": "user20@blueprint.io",
             "status": {"state": "STOPPED"}},
        ]
    },
    "serving": {
        "endpoints": [
            {"name": "fraud-detection-v2", "creator": "user20@blueprint.io",
             "config": {"served_entities": [{"entity_name": "fraud_model_v2", "entity_version": "3",
                         "foundation_model": {"display_name": "Fraud Detection v2", "input_price": "0.50", "price": "1.20", "pricing_model": "TOKEN"}}]},
             "state": {"ready": "READY"}, "creation_timestamp": 1700000000000,
             "capabilities": {"completions": True, "chat": False}},
            {"name": "churn-predictor", "creator": "user25@blueprint.io",
             "config": {"served_entities": [{"entity_name": "churn_xgb", "entity_version": "7",
                         "foundation_model": {"display_name": "Churn XGBoost", "input_price": "0.30", "price": "0.80", "pricing_model": "TOKEN"}}]},
             "state": {"ready": "READY"}, "creation_timestamp": 1705000000000,
             "capabilities": {"completions": True, "chat": True}},
            {"name": "text-embeddings", "creator": "user22@blueprint.io",
             "config": {"served_entities": [{"entity_name": "bge-large-en", "entity_version": "1",
                         "foundation_model": {"display_name": "BGE Large EN", "input_price": "0.10", "price": "0.10", "pricing_model": "TOKEN"}}]},
             "state": {"ready": "NOT_READY"}, "creation_timestamp": 1710000000000,
             "capabilities": {"embeddings": True}},
        ]
    },
}

# --- Realistic demo usage data ---
import random
random.seed(42)

days = [f"2026-03-{d:02d}" for d in range(1, 27)]
user_names = [f"user{i}@blueprint.io" for i in range(1, 20)]

# user_queries rows: (executed_by, execution_status, cnt, total_read_gb, total_rows, total_minutes)
user_query_rows = []
for u in user_names:
    cnt = random.randint(20, 350)
    user_query_rows.append([u, "FINISHED", cnt, round(random.uniform(0.5, 120.0), 4),
                            random.randint(50000, 80000000), round(random.uniform(5, 200), 2)])
    err = random.randint(0, 15)
    if err > 0:
        user_query_rows.append([u, "FAILED", err, 0, 0, round(random.uniform(0.1, 5), 2)])

# daily_queries rows: (query_date, total_queries, succeeded, failed, read_gb, read_rows, total_minutes)
daily_rows = []
for d in days:
    total = random.randint(120, 580)
    failed = random.randint(2, 25)
    daily_rows.append([d, total, total - failed, failed,
                       round(random.uniform(1.0, 80.0), 4),
                       random.randint(1000000, 200000000),
                       round(random.uniform(30, 600), 2)])

# warehouse_events rows: (warehouse_id, event_type, event_time, cluster_count)
wh_event_rows = []
for d in days:
    for _ in range(random.randint(2, 6)):
        wh_event_rows.append([
            random.choice(["wh-001-abcdef12", "wh-002-12345678", "wh-003-aabbccdd"]),
            random.choice(["STARTING", "RUNNING", "STOPPING", "SCALED_UP", "SCALED_DOWN"]),
            f"{d}T{random.randint(6,22):02d}:{random.randint(0,59):02d}:00Z",
            random.randint(1, 4),
        ])

# list_prices rows: (sku_name, price_usd, usage_unit)
price_rows = [
    ["ENTERPRISE_ALL_PURPOSE_COMPUTE", 0.55, "DBU"],
    ["ENTERPRISE_JOBS_COMPUTE", 0.30, "DBU"],
    ["ENTERPRISE_SQL_COMPUTE", 0.22, "DBU"],
    ["ENTERPRISE_SERVERLESS_SQL", 0.70, "DBU"],
    ["ENTERPRISE_MODEL_SERVING", 0.07, "DBU"],
    ["ENTERPRISE_DLT_CORE", 0.20, "DBU"],
    ["ENTERPRISE_DLT_PRO", 0.25, "DBU"],
    ["ENTERPRISE_JOBS_LIGHT_COMPUTE", 0.15, "DBU"],
    ["ENTERPRISE_SERVERLESS_REALTIME_INFERENCE", 0.08, "DBU"],
    ["ENTERPRISE_APPS", 0.06, "DBU"],
    ["ENTERPRISE_ANTHROPIC_CLAUDE_SONNET", 0.045, "TOKEN"],
]

# table_inventory: per-schema, rows = (table_name, table_type, created, last_altered, comment)
table_schemas = {
    "raw_data.landing": [
        ["raw_transactions", "MANAGED", "2025-06-01", "2026-03-25", "Raw transaction feed"],
        ["raw_customers", "MANAGED", "2025-06-01", "2026-03-25", "CRM customer dump"],
        ["raw_products", "MANAGED", "2025-06-15", "2026-03-20", "Product catalog feed"],
        ["raw_events", "MANAGED", "2025-07-01", "2026-03-25", "Clickstream events"],
        ["raw_clickstream", "EXTERNAL", "2025-08-01", "2026-03-24", "Web analytics raw"],
    ],
    "raw_data.bronze": [
        ["transactions", "MANAGED", "2025-06-02", "2026-03-25", "Deduplicated transactions"],
        ["customers", "MANAGED", "2025-06-02", "2026-03-25", "Cleaned customer records"],
        ["products", "MANAGED", "2025-06-16", "2026-03-20", "Product master"],
        ["events", "MANAGED", "2025-07-02", "2026-03-25", "Parsed events"],
        ["clickstream", "MANAGED", "2025-08-02", "2026-03-24", "Sessionized clicks"],
        ["orders", "MANAGED", "2025-09-01", "2026-03-25", "Order records"],
    ],
    "curated.silver": [
        ["dim_customer", "MANAGED", "2025-06-10", "2026-03-25", "Customer dimension"],
        ["dim_product", "MANAGED", "2025-06-10", "2026-03-20", "Product dimension"],
        ["fact_transaction", "MANAGED", "2025-06-10", "2026-03-25", "Transaction facts"],
        ["fact_event", "MANAGED", "2025-07-10", "2026-03-25", "Event facts"],
        ["dim_date", "MANAGED", "2025-06-10", "2025-12-31", "Date dimension"],
    ],
    "curated.gold": [
        ["daily_revenue", "MANAGED", "2025-07-01", "2026-03-25", "Daily revenue aggregates"],
        ["customer_ltv", "MANAGED", "2025-08-01", "2026-03-22", "Customer lifetime value"],
        ["product_performance", "MANAGED", "2025-08-01", "2026-03-20", "Product KPIs"],
        ["churn_scores", "MANAGED", "2025-09-15", "2026-03-25", "ML churn predictions"],
    ],
    "ml_features.features": [
        ["customer_features", "MANAGED", "2025-09-01", "2026-03-25", "Customer feature vectors"],
        ["transaction_features", "MANAGED", "2025-09-01", "2026-03-25", "Transaction features"],
        ["behavioral_features", "MANAGED", "2025-10-01", "2026-03-24", "Behavioral signals"],
    ],
    "ml_features.training": [
        ["fraud_training_set", "MANAGED", "2025-10-15", "2026-03-15", "Fraud model training data"],
        ["churn_training_set", "MANAGED", "2025-11-01", "2026-03-10", "Churn model training data"],
    ],
    "sandbox.experiments": [
        ["test_output_1", "MANAGED", "2026-01-15", "2026-03-20", "Experiment results"],
        ["test_output_2", "MANAGED", "2026-02-01", "2026-03-18", "A/B test output"],
        ["model_comparison", "MANAGED", "2026-02-15", "2026-03-22", "Model eval comparison"],
    ],
}

table_inventory = {}
table_sizes = {}
for key, rows in table_schemas.items():
    table_inventory[key] = {
        "cols": ["table_name", "table_type", "created", "last_altered", "comment"],
        "rows": rows,
    }
    for r in rows:
        fqn = f"{key}.{r[0]}"
        table_sizes[fqn] = random.randint(1000000, 50000000000)

usage_data = {
    "warehouse_id": "wh-001-abcdef12",
    "user_queries": {
        "cols": ["executed_by", "execution_status", "cnt", "total_read_gb", "total_rows", "total_minutes"],
        "rows": user_query_rows,
    },
    "daily_queries": {
        "cols": ["query_date", "total_queries", "succeeded", "failed", "read_gb", "read_rows", "total_minutes"],
        "rows": daily_rows,
    },
    "warehouse_events": {
        "cols": ["warehouse_id", "event_type", "event_time", "cluster_count"],
        "rows": wh_event_rows,
    },
    "list_prices": {
        "cols": ["sku_name", "price_usd", "usage_unit"],
        "rows": price_rows,
    },
    "schema_overview": {
        "cols": ["table_catalog", "table_schema", "table_count"],
        "rows": [
            ["raw_data", "landing", 5], ["raw_data", "bronze", 6],
            ["curated", "silver", 5], ["curated", "gold", 4],
            ["ml_features", "features", 3], ["ml_features", "training", 2],
            ["sandbox", "experiments", 3],
        ],
    },
    "table_inventory": table_inventory,
    "table_sizes": table_sizes,
}

# --- Save snapshot ---
snapshot = {"security": sec_data, "usage": usage_data}
save_data_snapshot(snapshot, duration_sec=42.7)

print("Demo data seeded successfully!")
print("Run: python3 -m app.main --no-browser --port 8050")
