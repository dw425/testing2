"""
Data writers for LHO Lite — Delta Tables and Lakebase.

Writes collected + analyzed data to external destinations after each refresh:
  - DeltaWriter: Writes via SQL Statement API (reuses collector's sql() method)
  - LakebaseWriter: Writes via pg8000 PostgreSQL wire protocol

10 tables are written per snapshot:
  snapshots, security_findings, security_scores, compliance,
  workspace_profile, user_activity, daily_queries, billing_daily,
  billing_by_product, job_runs
"""

import json
import logging
import uuid
from datetime import datetime, timezone

log = logging.getLogger("lho.data_writer")


# ---------------------------------------------------------------------------
# SQL value helpers
# ---------------------------------------------------------------------------

def _sql_str(v) -> str:
    """Escape a value as a SQL string literal."""
    if v is None:
        return "NULL"
    s = str(v).replace("\\", "\\\\").replace("'", "''")
    return f"'{s}'"


def _sql_val(v) -> str:
    """Format a value for a SQL INSERT."""
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return str(v)
    return _sql_str(v)


def _safe(v, default=""):
    """Coerce to string safely."""
    if v is None:
        return default
    return v


# ---------------------------------------------------------------------------
# Delta Table DDL
# ---------------------------------------------------------------------------

_DELTA_TABLES = {
    "snapshots": """(
        snapshot_id STRING NOT NULL,
        snapshot_ts TIMESTAMP,
        duration_sec DOUBLE,
        workspace_url STRING,
        cloud STRING,
        score INT,
        grade STRING,
        total_findings INT,
        critical_count INT,
        high_count INT,
        medium_count INT,
        low_count INT
    )""",
    "security_findings": """(
        snapshot_id STRING NOT NULL,
        snapshot_ts TIMESTAMP,
        finding_id STRING,
        severity STRING,
        category STRING,
        nist_controls STRING,
        finding STRING,
        impact STRING,
        recommendation STRING
    )""",
    "security_scores": """(
        snapshot_id STRING NOT NULL,
        snapshot_ts TIMESTAMP,
        score INT,
        grade STRING,
        total_findings INT,
        critical_count INT,
        high_count INT,
        medium_count INT,
        low_count INT
    )""",
    "compliance": """(
        snapshot_id STRING NOT NULL,
        snapshot_ts TIMESTAMP,
        framework STRING,
        status STRING,
        score INT,
        controls_total INT,
        controls_passed INT,
        controls_json STRING
    )""",
    "workspace_profile": """(
        snapshot_id STRING NOT NULL,
        snapshot_ts TIMESTAMP,
        cloud STRING,
        region STRING,
        workspace_url STRING,
        is_govcloud BOOLEAN,
        total_users INT,
        admin_count INT,
        group_count INT,
        sp_count INT,
        cluster_count INT,
        warehouse_count INT,
        job_count INT,
        app_count INT,
        catalog_count INT,
        metastore_count INT,
        ip_list_count INT,
        secret_scope_count INT,
        storage_cred_count INT,
        ext_location_count INT,
        share_count INT
    )""",
    "user_activity": """(
        snapshot_id STRING NOT NULL,
        snapshot_ts TIMESTAMP,
        executed_by STRING,
        execution_status STRING,
        query_count BIGINT,
        total_read_gb DOUBLE,
        total_rows BIGINT,
        total_minutes DOUBLE
    )""",
    "daily_queries": """(
        snapshot_id STRING NOT NULL,
        snapshot_ts TIMESTAMP,
        query_date STRING,
        total_queries INT,
        succeeded INT,
        failed INT,
        read_gb DOUBLE,
        read_rows BIGINT,
        total_minutes DOUBLE
    )""",
    "billing_daily": """(
        snapshot_id STRING NOT NULL,
        snapshot_ts TIMESTAMP,
        usage_date STRING,
        total_dbus DOUBLE,
        sql_dbus DOUBLE,
        jobs_dbus DOUBLE,
        allpurpose_dbus DOUBLE,
        dlt_dbus DOUBLE,
        other_dbus DOUBLE
    )""",
    "billing_by_product": """(
        snapshot_id STRING NOT NULL,
        snapshot_ts TIMESTAMP,
        product STRING,
        total_dbus DOUBLE,
        active_days INT
    )""",
    "job_runs": """(
        snapshot_id STRING NOT NULL,
        snapshot_ts TIMESTAMP,
        job_id STRING,
        job_name STRING,
        total_runs INT,
        succeeded INT,
        failed INT,
        canceled INT,
        avg_duration_min DOUBLE,
        total_duration_min DOUBLE
    )""",
}


# ---------------------------------------------------------------------------
# Lakebase (PostgreSQL) DDL
# ---------------------------------------------------------------------------

_PG_TABLES = {
    "snapshots": """(
        snapshot_id TEXT NOT NULL PRIMARY KEY,
        snapshot_ts TIMESTAMPTZ,
        duration_sec DOUBLE PRECISION,
        workspace_url TEXT,
        cloud TEXT,
        score INTEGER,
        grade TEXT,
        total_findings INTEGER,
        critical_count INTEGER,
        high_count INTEGER,
        medium_count INTEGER,
        low_count INTEGER
    )""",
    "security_findings": """(
        snapshot_id TEXT NOT NULL,
        snapshot_ts TIMESTAMPTZ,
        finding_id TEXT,
        severity TEXT,
        category TEXT,
        nist_controls TEXT,
        finding TEXT,
        impact TEXT,
        recommendation TEXT
    )""",
    "security_scores": """(
        snapshot_id TEXT NOT NULL,
        snapshot_ts TIMESTAMPTZ,
        score INTEGER,
        grade TEXT,
        total_findings INTEGER,
        critical_count INTEGER,
        high_count INTEGER,
        medium_count INTEGER,
        low_count INTEGER
    )""",
    "compliance": """(
        snapshot_id TEXT NOT NULL,
        snapshot_ts TIMESTAMPTZ,
        framework TEXT,
        status TEXT,
        score INTEGER,
        controls_total INTEGER,
        controls_passed INTEGER,
        controls_json TEXT
    )""",
    "workspace_profile": """(
        snapshot_id TEXT NOT NULL,
        snapshot_ts TIMESTAMPTZ,
        cloud TEXT,
        region TEXT,
        workspace_url TEXT,
        is_govcloud BOOLEAN,
        total_users INTEGER,
        admin_count INTEGER,
        group_count INTEGER,
        sp_count INTEGER,
        cluster_count INTEGER,
        warehouse_count INTEGER,
        job_count INTEGER,
        app_count INTEGER,
        catalog_count INTEGER,
        metastore_count INTEGER,
        ip_list_count INTEGER,
        secret_scope_count INTEGER,
        storage_cred_count INTEGER,
        ext_location_count INTEGER,
        share_count INTEGER
    )""",
    "user_activity": """(
        snapshot_id TEXT NOT NULL,
        snapshot_ts TIMESTAMPTZ,
        executed_by TEXT,
        execution_status TEXT,
        query_count BIGINT,
        total_read_gb DOUBLE PRECISION,
        total_rows BIGINT,
        total_minutes DOUBLE PRECISION
    )""",
    "daily_queries": """(
        snapshot_id TEXT NOT NULL,
        snapshot_ts TIMESTAMPTZ,
        query_date TEXT,
        total_queries INTEGER,
        succeeded INTEGER,
        failed INTEGER,
        read_gb DOUBLE PRECISION,
        read_rows BIGINT,
        total_minutes DOUBLE PRECISION
    )""",
    "billing_daily": """(
        snapshot_id TEXT NOT NULL,
        snapshot_ts TIMESTAMPTZ,
        usage_date TEXT,
        total_dbus DOUBLE PRECISION,
        sql_dbus DOUBLE PRECISION,
        jobs_dbus DOUBLE PRECISION,
        allpurpose_dbus DOUBLE PRECISION,
        dlt_dbus DOUBLE PRECISION,
        other_dbus DOUBLE PRECISION
    )""",
    "billing_by_product": """(
        snapshot_id TEXT NOT NULL,
        snapshot_ts TIMESTAMPTZ,
        product TEXT,
        total_dbus DOUBLE PRECISION,
        active_days INTEGER
    )""",
    "job_runs": """(
        snapshot_id TEXT NOT NULL,
        snapshot_ts TIMESTAMPTZ,
        job_id TEXT,
        job_name TEXT,
        total_runs INTEGER,
        succeeded INTEGER,
        failed INTEGER,
        canceled INTEGER,
        avg_duration_min DOUBLE PRECISION,
        total_duration_min DOUBLE PRECISION
    )""",
}


# ---------------------------------------------------------------------------
# DeltaWriter
# ---------------------------------------------------------------------------

class DeltaWriter:
    """Writes LHO data to Delta tables via the SQL Statement API."""

    def __init__(self, collector, config: dict, snapshot_id: str, snapshot_ts: str):
        self.collector = collector
        self.catalog = config.get("dest_catalog", "lho_data")
        self.schema = config.get("dest_schema", "lho_lite")
        self.prefix = config.get("dest_table_prefix", "lho_")
        self.snapshot_id = snapshot_id
        self.snapshot_ts = snapshot_ts
        self._wh_id = None

    def _find_warehouse(self, data: dict) -> str | None:
        wh_id = data.get("usage", {}).get("warehouse_id")
        if wh_id:
            return wh_id
        warehouses = data.get("security", {}).get("warehouses", {}).get("warehouses", [])
        for w in warehouses:
            if w.get("state") == "RUNNING":
                return w["id"]
        if warehouses:
            return warehouses[0]["id"]
        return None

    def _sql(self, statement: str):
        return self.collector.sql(self._wh_id, statement)

    def _fqn(self, name: str) -> str:
        return f"`{self.catalog}`.`{self.schema}`.`{self.prefix}{name}`"

    def _ensure_schema(self):
        self._sql(f"CREATE SCHEMA IF NOT EXISTS `{self.catalog}`.`{self.schema}`")

    def _create_tables(self):
        for name, ddl in _DELTA_TABLES.items():
            self._sql(f"CREATE TABLE IF NOT EXISTS {self._fqn(name)} {ddl}")

    def _insert(self, table: str, columns: list[str], rows: list[list]):
        if not rows:
            return
        fqn = self._fqn(table)
        col_clause = ", ".join(columns)
        for i in range(0, len(rows), 100):
            batch = rows[i:i + 100]
            values = ", ".join(
                "(" + ", ".join(_sql_val(v) for v in row) + ")"
                for row in batch
            )
            self._sql(f"INSERT INTO {fqn} ({col_clause}) VALUES {values}")

    # ---- Write methods ----

    def _write_snapshot(self, data, score, duration):
        sec = data.get("security", {})
        self._insert("snapshots",
            ["snapshot_id", "snapshot_ts", "duration_sec", "workspace_url", "cloud",
             "score", "grade", "total_findings", "critical_count", "high_count",
             "medium_count", "low_count"],
            [[self.snapshot_id, self.snapshot_ts, duration,
              sec.get("_workspace_url", ""), sec.get("_cloud", ""),
              score["score"], score["grade"], score["total_findings"],
              score["critical"], score["high"], score["medium"], score["low"]]])

    def _write_findings(self, findings):
        rows = []
        for i, f in enumerate(findings, 1):
            rows.append([self.snapshot_id, self.snapshot_ts, f"SEC-{i:03d}",
                         f[0], f[1], f[2], f[3], f[4], f[5]])
        self._insert("security_findings",
            ["snapshot_id", "snapshot_ts", "finding_id", "severity", "category",
             "nist_controls", "finding", "impact", "recommendation"], rows)

    def _write_scores(self, score):
        self._insert("security_scores",
            ["snapshot_id", "snapshot_ts", "score", "grade", "total_findings",
             "critical_count", "high_count", "medium_count", "low_count"],
            [[self.snapshot_id, self.snapshot_ts, score["score"], score["grade"],
              score["total_findings"], score["critical"], score["high"],
              score["medium"], score["low"]]])

    def _write_compliance(self, compliance):
        rows = []
        for framework, info in compliance.items():
            controls = info.get("controls", [])
            passed = sum(1 for c in controls if c["status"] == "PASS")
            rows.append([
                self.snapshot_id, self.snapshot_ts, framework,
                info["status"], info["score"], len(controls), passed,
                json.dumps(controls),
            ])
        self._insert("compliance",
            ["snapshot_id", "snapshot_ts", "framework", "status", "score",
             "controls_total", "controls_passed", "controls_json"], rows)

    def _write_workspace_profile(self, profile):
        self._insert("workspace_profile",
            ["snapshot_id", "snapshot_ts", "cloud", "region", "workspace_url",
             "is_govcloud", "total_users", "admin_count", "group_count", "sp_count",
             "cluster_count", "warehouse_count", "job_count", "app_count",
             "catalog_count", "metastore_count", "ip_list_count",
             "secret_scope_count", "storage_cred_count", "ext_location_count",
             "share_count"],
            [[self.snapshot_id, self.snapshot_ts,
              profile.get("cloud"), profile.get("region"),
              profile.get("workspace_url"), profile.get("is_govcloud", False),
              profile.get("total_users", 0), profile.get("admin_count", 0),
              profile.get("group_count", 0), profile.get("sp_count", 0),
              profile.get("cluster_count", 0), profile.get("warehouse_count", 0),
              profile.get("job_count", 0), profile.get("app_count", 0),
              profile.get("catalog_count", 0), profile.get("metastore_count", 0),
              profile.get("ip_list_count", 0), profile.get("secret_scope_count", 0),
              profile.get("storage_cred_count", 0), profile.get("ext_location_count", 0),
              profile.get("share_count", 0)]])

    def _write_usage(self, usage):
        sid, ts = self.snapshot_id, self.snapshot_ts

        # user_activity
        uq = usage.get("user_queries", {})
        if uq.get("rows"):
            rows = [[sid, ts] + [_safe(v) for v in r[:6]] for r in uq["rows"]]
            self._insert("user_activity",
                ["snapshot_id", "snapshot_ts", "executed_by", "execution_status",
                 "query_count", "total_read_gb", "total_rows", "total_minutes"], rows)

        # daily_queries
        dq = usage.get("daily_queries", {})
        if dq.get("rows"):
            rows = [[sid, ts] + [_safe(v) for v in r[:7]] for r in dq["rows"]]
            self._insert("daily_queries",
                ["snapshot_id", "snapshot_ts", "query_date", "total_queries",
                 "succeeded", "failed", "read_gb", "read_rows", "total_minutes"], rows)

        # billing_daily
        dc = usage.get("daily_cost", {})
        if dc.get("rows"):
            rows = [[sid, ts] + [_safe(v) for v in r[:7]] for r in dc["rows"]]
            self._insert("billing_daily",
                ["snapshot_id", "snapshot_ts", "usage_date", "total_dbus",
                 "sql_dbus", "jobs_dbus", "allpurpose_dbus", "dlt_dbus", "other_dbus"], rows)

        # billing_by_product
        cp = usage.get("cost_by_product", {})
        if cp.get("rows"):
            rows = [[sid, ts] + [_safe(v) for v in r[:3]] for r in cp["rows"]]
            self._insert("billing_by_product",
                ["snapshot_id", "snapshot_ts", "product", "total_dbus", "active_days"], rows)

        # job_runs
        jr = usage.get("job_runs", {})
        if jr.get("rows"):
            rows = [[sid, ts] + [_safe(v) for v in r[:8]] for r in jr["rows"]]
            self._insert("job_runs",
                ["snapshot_id", "snapshot_ts", "job_id", "job_name", "total_runs",
                 "succeeded", "failed", "canceled", "avg_duration_min",
                 "total_duration_min"], rows)

    # ---- Main entry point ----

    def write_all(self, data, findings, security_score, compliance, workspace_profile, duration):
        self._wh_id = self._find_warehouse(data)
        if not self._wh_id:
            log.error("DeltaWriter: no SQL warehouse available — skipping write")
            return

        log.info("DeltaWriter: writing to %s.%s (prefix=%s)", self.catalog, self.schema, self.prefix)
        self._ensure_schema()
        self._create_tables()
        self._write_snapshot(data, security_score, duration)
        self._write_findings(findings)
        self._write_scores(security_score)
        self._write_compliance(compliance)
        self._write_workspace_profile(workspace_profile)
        self._write_usage(data.get("usage", {}))
        log.info("DeltaWriter: 10 tables written successfully")


# ---------------------------------------------------------------------------
# LakebaseWriter
# ---------------------------------------------------------------------------

class LakebaseWriter:
    """Writes LHO data to Lakebase via pg8000 PostgreSQL wire protocol."""

    def __init__(self, config: dict, snapshot_id: str, snapshot_ts: str):
        self.instance = config.get("lakebase_instance", "")
        self.lb_schema = config.get("lakebase_schema", "public")
        self.prefix = config.get("dest_table_prefix", "lho_")
        self.snapshot_id = snapshot_id
        self.snapshot_ts = snapshot_ts
        self._config = config
        self._conn = None

    def _connect(self):
        import pg8000
        token = self._config.get("pat_token", "")
        host = self.instance
        workspace_url = self._config.get("workspace_url", "")
        # If instance is a bare name, derive host from workspace URL
        if host and "." not in host and workspace_url:
            from urllib.parse import urlparse
            ws_host = urlparse(workspace_url).hostname or workspace_url
            host = ws_host

        self._conn = pg8000.connect(
            host=host,
            port=443,
            user="token",
            password=token,
            database=self.instance,
            ssl_context=True,
        )

    def _fqn(self, name: str) -> str:
        return f"{self.lb_schema}.{self.prefix}{name}"

    def _create_tables(self):
        cur = self._conn.cursor()
        if self.lb_schema != "public":
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.lb_schema}")
        for name, ddl in _PG_TABLES.items():
            cur.execute(f"CREATE TABLE IF NOT EXISTS {self._fqn(name)} {ddl}")

    def _insert(self, table: str, columns: list[str], rows: list[list]):
        if not rows:
            return
        fqn = self._fqn(table)
        col_clause = ", ".join(columns)
        placeholders = ", ".join(f"%s" for _ in columns)
        cur = self._conn.cursor()
        for row in rows:
            cur.execute(f"INSERT INTO {fqn} ({col_clause}) VALUES ({placeholders})", row)

    # ---- Write methods (mirror DeltaWriter) ----

    def _write_snapshot(self, data, score, duration):
        sec = data.get("security", {})
        self._insert("snapshots",
            ["snapshot_id", "snapshot_ts", "duration_sec", "workspace_url", "cloud",
             "score", "grade", "total_findings", "critical_count", "high_count",
             "medium_count", "low_count"],
            [[self.snapshot_id, self.snapshot_ts, duration,
              sec.get("_workspace_url", ""), sec.get("_cloud", ""),
              score["score"], score["grade"], score["total_findings"],
              score["critical"], score["high"], score["medium"], score["low"]]])

    def _write_findings(self, findings):
        rows = []
        for i, f in enumerate(findings, 1):
            rows.append([self.snapshot_id, self.snapshot_ts, f"SEC-{i:03d}",
                         f[0], f[1], f[2], f[3], f[4], f[5]])
        self._insert("security_findings",
            ["snapshot_id", "snapshot_ts", "finding_id", "severity", "category",
             "nist_controls", "finding", "impact", "recommendation"], rows)

    def _write_scores(self, score):
        self._insert("security_scores",
            ["snapshot_id", "snapshot_ts", "score", "grade", "total_findings",
             "critical_count", "high_count", "medium_count", "low_count"],
            [[self.snapshot_id, self.snapshot_ts, score["score"], score["grade"],
              score["total_findings"], score["critical"], score["high"],
              score["medium"], score["low"]]])

    def _write_compliance(self, compliance):
        rows = []
        for framework, info in compliance.items():
            controls = info.get("controls", [])
            passed = sum(1 for c in controls if c["status"] == "PASS")
            rows.append([
                self.snapshot_id, self.snapshot_ts, framework,
                info["status"], info["score"], len(controls), passed,
                json.dumps(controls),
            ])
        self._insert("compliance",
            ["snapshot_id", "snapshot_ts", "framework", "status", "score",
             "controls_total", "controls_passed", "controls_json"], rows)

    def _write_workspace_profile(self, profile):
        self._insert("workspace_profile",
            ["snapshot_id", "snapshot_ts", "cloud", "region", "workspace_url",
             "is_govcloud", "total_users", "admin_count", "group_count", "sp_count",
             "cluster_count", "warehouse_count", "job_count", "app_count",
             "catalog_count", "metastore_count", "ip_list_count",
             "secret_scope_count", "storage_cred_count", "ext_location_count",
             "share_count"],
            [[self.snapshot_id, self.snapshot_ts,
              profile.get("cloud"), profile.get("region"),
              profile.get("workspace_url"), profile.get("is_govcloud", False),
              profile.get("total_users", 0), profile.get("admin_count", 0),
              profile.get("group_count", 0), profile.get("sp_count", 0),
              profile.get("cluster_count", 0), profile.get("warehouse_count", 0),
              profile.get("job_count", 0), profile.get("app_count", 0),
              profile.get("catalog_count", 0), profile.get("metastore_count", 0),
              profile.get("ip_list_count", 0), profile.get("secret_scope_count", 0),
              profile.get("storage_cred_count", 0), profile.get("ext_location_count", 0),
              profile.get("share_count", 0)]])

    def _write_usage(self, usage):
        sid, ts = self.snapshot_id, self.snapshot_ts

        uq = usage.get("user_queries", {})
        if uq.get("rows"):
            rows = [[sid, ts] + [_safe(v) for v in r[:6]] for r in uq["rows"]]
            self._insert("user_activity",
                ["snapshot_id", "snapshot_ts", "executed_by", "execution_status",
                 "query_count", "total_read_gb", "total_rows", "total_minutes"], rows)

        dq = usage.get("daily_queries", {})
        if dq.get("rows"):
            rows = [[sid, ts] + [_safe(v) for v in r[:7]] for r in dq["rows"]]
            self._insert("daily_queries",
                ["snapshot_id", "snapshot_ts", "query_date", "total_queries",
                 "succeeded", "failed", "read_gb", "read_rows", "total_minutes"], rows)

        dc = usage.get("daily_cost", {})
        if dc.get("rows"):
            rows = [[sid, ts] + [_safe(v) for v in r[:7]] for r in dc["rows"]]
            self._insert("billing_daily",
                ["snapshot_id", "snapshot_ts", "usage_date", "total_dbus",
                 "sql_dbus", "jobs_dbus", "allpurpose_dbus", "dlt_dbus", "other_dbus"], rows)

        cp = usage.get("cost_by_product", {})
        if cp.get("rows"):
            rows = [[sid, ts] + [_safe(v) for v in r[:3]] for r in cp["rows"]]
            self._insert("billing_by_product",
                ["snapshot_id", "snapshot_ts", "product", "total_dbus", "active_days"], rows)

        jr = usage.get("job_runs", {})
        if jr.get("rows"):
            rows = [[sid, ts] + [_safe(v) for v in r[:8]] for r in jr["rows"]]
            self._insert("job_runs",
                ["snapshot_id", "snapshot_ts", "job_id", "job_name", "total_runs",
                 "succeeded", "failed", "canceled", "avg_duration_min",
                 "total_duration_min"], rows)

    # ---- Main entry point ----

    def write_all(self, data, findings, security_score, compliance, workspace_profile, duration):
        log.info("LakebaseWriter: connecting to %s (schema=%s)", self.instance, self.lb_schema)
        try:
            self._connect()
            self._create_tables()
            self._write_snapshot(data, security_score, duration)
            self._write_findings(findings)
            self._write_scores(security_score)
            self._write_compliance(compliance)
            self._write_workspace_profile(workspace_profile)
            self._write_usage(data.get("usage", {}))
            self._conn.commit()
            log.info("LakebaseWriter: 10 tables written successfully")
        except Exception:
            if self._conn:
                try:
                    self._conn.rollback()
                except Exception:
                    pass
            raise
        finally:
            if self._conn:
                try:
                    self._conn.close()
                except Exception:
                    pass


# ---------------------------------------------------------------------------
# Factory — called from main.py after each refresh
# ---------------------------------------------------------------------------

def write_to_destinations(collector, data, findings, security_score, compliance, workspace_profile, duration):
    """Write collected + analyzed data to configured external destinations."""
    from app.config_store import get_config
    cfg = get_config()
    dest = cfg.get("data_destination", "local")

    if dest == "local":
        return

    snapshot_id = str(uuid.uuid4())
    snapshot_ts = datetime.now(timezone.utc).isoformat()

    if dest in ("delta", "both"):
        try:
            writer = DeltaWriter(collector, cfg, snapshot_id, snapshot_ts)
            writer.write_all(data, findings, security_score, compliance, workspace_profile, duration)
        except Exception as e:
            log.error("DeltaWriter failed: %s", e)

    if dest in ("lakebase", "both"):
        if not cfg.get("lakebase_instance"):
            log.warning("Lakebase destination configured but no instance specified — skipping")
            return
        try:
            writer = LakebaseWriter(cfg, snapshot_id, snapshot_ts)
            writer.write_all(data, findings, security_score, compliance, workspace_profile, duration)
        except ImportError:
            log.error("LakebaseWriter requires pg8000: pip install pg8000")
        except Exception as e:
            log.error("LakebaseWriter failed: %s", e)
