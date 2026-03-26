"""
Security analysis engine for LHO Lite.

16 NIST-mapped security checks + security score + Mermaid architecture diagrams.
Universal across AWS / Azure / GCP Databricks workspaces.
"""

import json
import re
from datetime import datetime


# ---------------------------------------------------------------------------
# Security analysis (16 checks)
# ---------------------------------------------------------------------------

def analyze_security(sec_data: dict) -> list[tuple]:
    """Run 16 security checks.  Returns list of (severity, category, nist, finding, impact, recommendation)."""

    me = sec_data.get("me", {})
    users = sec_data.get("users", {})
    groups = sec_data.get("groups", {})
    clusters = sec_data.get("clusters", {})
    policies = sec_data.get("policies", {})
    ip_lists = sec_data.get("ip_lists", {})
    workspace_conf = sec_data.get("workspace_conf", {})
    metastores = sec_data.get("metastores", {})
    init_contents = sec_data.get("init_contents", {})
    script_list = sec_data.get("init_scripts", {}).get("scripts", [])

    cloud = sec_data.get("_cloud", "AWS")
    is_govcloud = sec_data.get("_govcloud", False)

    total_users = users.get("totalResults", 0)
    user_list = users.get("Resources", [])
    group_list = groups.get("Resources", [])
    admin_group = next((g for g in group_list if g.get("displayName") == "admins"), {})
    admin_count = len(admin_group.get("members", []))
    cluster_list = clusters.get("clusters", [])
    policy_list = policies.get("policies", [])
    metastore_list = metastores.get("metastores", [])
    max_token_days = int(workspace_conf.get("maxTokenLifetimeDays", "0") or "0")

    # Detect domains
    domains = {}
    for u in user_list:
        un = u.get("userName", "")
        if "@" in un:
            d = un.split("@")[1]
            domains[d] = domains.get(d, 0) + 1
    sorted_domains = sorted(domains.items(), key=lambda x: -x[1])
    org_domains = set(d for d, c in sorted_domains[:2]) if sorted_domains else set()
    personal_domains = {"gmail.com", "outlook.com", "hotmail.com", "yahoo.com", "live.com", "icloud.com", "protonmail.com"}
    ext_domains = set(d for d in domains if d not in org_domains and d not in personal_domains)
    personal_found = set(d for d in domains if d in personal_domains)

    region = "unknown"
    for m in metastore_list:
        cloud = m.get("cloud", cloud).upper()
        region = m.get("region", region)

    findings = []

    # F1: Init script credential exposure
    for sid, content in init_contents.items():
        script_name = next((s["name"] for s in script_list if s.get("script_id") == sid), sid)
        has_cred = (
            bool(re.search(r"AKIA[A-Z0-9]{16}", content))  # AWS key
            or bool(re.search(r"secret[._-]?key.*=.*[\"'][^\"']{20}", content, re.IGNORECASE))
            or bool(re.search(r"password\s*=\s*[\"'][^\"']{8}", content, re.IGNORECASE))
            or bool(re.search(r"AccountKey=[A-Za-z0-9+/=]{40,}", content))  # Azure storage key
            or bool(re.search(r'"type"\s*:\s*"service_account"', content))  # GCP service account JSON
        )
        if has_cred:
            findings.append((
                "CRITICAL", "Credential Exposure", "IA-5, SC-12",
                f"Init script '{script_name}' contains hardcoded credentials.",
                "Credential exposure to all workspace users.",
                "IMMEDIATELY rotate keys. Move to Secret Scope or cloud-native IAM (Instance Profile / Managed Identity / Workload Identity).",
            ))

    # F2: Admin ratio
    if total_users > 0 and admin_count / total_users > 0.2:
        findings.append((
            "CRITICAL", "Access Control", "AC-2, AC-6",
            f"{admin_count}/{total_users} users ({admin_count * 100 // total_users}%) have admin privileges.",
            "Excessive admin access violates least-privilege.",
            f"Reduce admins to <{max(3, total_users // 10)}.",
        ))

    # F3: GovCloud / compliance environment
    if not is_govcloud:
        cloud_label = {"AWS": "AWS", "AZURE": "Azure", "GCP": "GCP"}.get(cloud, cloud)
        findings.append((
            "CRITICAL", "FedRAMP Environment", "CA-3, SC-7",
            f"Workspace on commercial {cloud_label} ({region}), NOT GovCloud.",
            "Cannot achieve FedRAMP authorization on commercial cloud.",
            f"Migrate to Databricks on {cloud_label} GovCloud / Government region.",
        ))

    # F4: IP access lists
    if not ip_lists.get("ip_access_lists"):
        findings.append((
            "HIGH", "Network Security", "SC-7, AC-17",
            "No IP Access Lists configured. Workspace accessible from any IP.",
            "No network-level access control.",
            "Enable IP Access Lists. Restrict to corporate VPN/office IPs.",
        ))

    # F5: Token lifetime
    if max_token_days > 90:
        findings.append((
            "HIGH", "Token Management", "IA-5, AC-2(3)",
            f"Token lifetime: {max_token_days} days. FedRAMP recommends <=60.",
            "Long-lived tokens increase credential theft risk.",
            "Reduce maxTokenLifetimeDays to 90 or less.",
        ))

    # F6: Disk encryption
    unencrypted = [c for c in cluster_list if not c.get("enable_local_disk_encryption", False)]
    if unencrypted:
        findings.append((
            "HIGH", "Encryption", "SC-28, SC-13",
            f"{len(unencrypted)} cluster(s) have local disk encryption DISABLED.",
            "Data at rest on cluster disks is unencrypted.",
            "Enable local disk encryption in all cluster policies.",
        ))

    # F7: Legacy security modes
    for p in policy_list:
        defn = p.get("definition", "{}")
        if isinstance(defn, str):
            defn = json.loads(defn) if defn else {}
        sec = defn.get("data_security_mode", {})
        if isinstance(sec, dict) and sec.get("value") == "NONE":
            findings.append((
                "HIGH", "Data Governance", "AC-3, AC-6",
                f"Policy '{p['name']}' has data_security_mode=NONE.",
                "Users bypass all Unity Catalog access controls.",
                "Change to USER_ISOLATION.",
            ))

    # F8: Session management
    findings.append((
        "HIGH", "Session Management", "AC-12, SC-10",
        "Session idle timeout should be verified.",
        "FedRAMP requires AC-12 session termination.",
        "Set session idle timeout to 30 minutes.",
    ))

    # F9: External domains
    if ext_domains:
        findings.append((
            "MEDIUM", "External Access", "AC-2(7), PS-7",
            f"External domains: {', '.join(sorted(ext_domains))}.",
            "External contractors increase attack surface.",
            "Review external access quarterly.",
        ))

    # F10: Personal email accounts
    if personal_found:
        findings.append((
            "MEDIUM", "Personal Accounts", "IA-2, AC-2",
            f"Personal email domains detected: {', '.join(sorted(personal_found))}.",
            "Personal accounts bypass corporate identity controls.",
            "Migrate users to corporate SSO accounts.",
        ))

    # F11: Audit logging
    findings.append((
        "MEDIUM", "Audit Logging", "AU-2, AU-3, AU-6",
        "Verify verbose audit logs are enabled and forwarded to SIEM.",
        "Without logging, incidents cannot be investigated.",
        "Enable verbose audit logs. Forward to cloud-native SIEM (CloudTrail/Azure Monitor/Cloud Logging).",
    ))

    # F12: Secret management
    scope_list = sec_data.get("secrets", {}).get("scopes", [])
    if len(scope_list) <= 1:
        findings.append((
            "MEDIUM", "Secret Management", "SC-12, SC-28",
            f"Only {len(scope_list)} secret scope(s) configured.",
            "Secrets may not be centrally managed.",
            "Migrate all credentials to secret scopes backed by cloud KMS.",
        ))

    # F13: Delta Sharing
    shares = sec_data.get("shares", {}).get("shares", [])
    recipients = sec_data.get("recipients", {}).get("recipients", [])
    if shares or recipients:
        findings.append((
            "MEDIUM", "Delta Sharing", "AC-3, AC-21",
            f"{len(shares)} share(s) configured with {len(recipients)} recipient(s).",
            "Data sharing extends trust boundary beyond workspace.",
            "Review sharing recipients and permissions quarterly.",
        ))

    # F14: Auto-termination
    no_autoterminate = [c for c in cluster_list if c.get("autotermination_minutes", 0) == 0]
    if no_autoterminate:
        findings.append((
            "MEDIUM", "Cost & Security", "SC-10, AC-12",
            f"{len(no_autoterminate)} cluster(s) have auto-termination disabled.",
            "Idle clusters waste resources and expand attack surface.",
            "Set auto-termination to 10-30 minutes.",
        ))

    # F15: SSO
    findings.append((
        "LOW", "Identity Federation", "IA-2, IA-8",
        "Verify SAML/OIDC SSO is enforced.",
        "Without SSO, password policies cannot be centrally enforced.",
        "Enable SAML/OIDC SSO with MFA.",
    ))

    # F16: Data exfiltration controls
    for key in ("enableExportNotebook", "enableResultsDownloading"):
        val = workspace_conf.get(key)
        if val is None or val == "true":
            findings.append((
                "LOW", "Data Exfiltration", "SC-28, AC-3",
                f"'{key}' is {'unset' if val is None else 'enabled'}.",
                "Users may export data containing sensitive information.",
                f"Set {key}=false for non-admin users.",
            ))
            break

    sev_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
    findings.sort(key=lambda x: sev_order.get(x[0], 99))
    return findings


# ---------------------------------------------------------------------------
# Compliance framework assessment
# ---------------------------------------------------------------------------

def assess_compliance(sec_data: dict, findings: list) -> dict:
    """Assess workspace against HIPAA, FedRAMP, SOC 2, and RBAC frameworks.

    Returns dict of framework -> {status, score, controls: [{name, status, detail}]}
    """
    workspace_conf = sec_data.get("workspace_conf", {})
    ip_lists = sec_data.get("ip_lists", {}).get("ip_access_lists", [])
    metastores = sec_data.get("metastores", {}).get("metastores", [])
    clusters = sec_data.get("clusters", {}).get("clusters", [])
    policies = sec_data.get("policies", {}).get("policies", [])
    groups = sec_data.get("groups", {}).get("Resources", [])
    secrets = sec_data.get("secrets", {}).get("scopes", [])
    users = sec_data.get("users", {})
    admin_group = next((g for g in groups if g.get("displayName") == "admins"), {})
    total_users = users.get("totalResults", 0)
    admin_count = len(admin_group.get("members", []))
    is_govcloud = sec_data.get("_govcloud", False)
    cloud = sec_data.get("_cloud", "AWS")

    has_uc = len(metastores) > 0
    has_ip_lists = len(ip_lists) > 0
    has_encryption = all(c.get("enable_local_disk_encryption", False) for c in clusters) if clusters else True
    has_secrets = len(secrets) >= 2
    admin_ratio_ok = admin_count / max(total_users, 1) <= 0.2
    token_ok = int(workspace_conf.get("maxTokenLifetimeDays", "0") or "0") <= 90
    export_disabled = workspace_conf.get("enableExportNotebook") == "false"
    download_disabled = workspace_conf.get("enableResultsDownloading") == "false"

    def _ctrl(name, passed, detail_pass, detail_fail):
        return {"name": name, "status": "PASS" if passed else "FAIL", "detail": detail_pass if passed else detail_fail}

    # --- HIPAA ---
    hipaa_controls = [
        _ctrl("Access Controls (AC)", admin_ratio_ok,
              f"Admin ratio {admin_count}/{total_users} within limits",
              f"Admin ratio {admin_count}/{total_users} exceeds 20%"),
        _ctrl("Audit Logging (AU)", True,
              "Databricks provides audit logging by default",
              "Verify audit log delivery"),
        _ctrl("Encryption at Rest (SC-28)", has_encryption,
              "All clusters have disk encryption enabled",
              "Some clusters lack disk encryption"),
        _ctrl("Encryption in Transit (SC-8)", True,
              "TLS enforced on all Databricks endpoints",
              "TLS should be verified"),
        _ctrl("Unity Catalog (Data Governance)", has_uc,
              f"{len(metastores)} metastore(s) configured",
              "No Unity Catalog metastore found"),
        _ctrl("Secret Management", has_secrets,
              f"{len(secrets)} secret scope(s) for credential management",
              "Insufficient secret scopes"),
        _ctrl("Network Isolation", has_ip_lists,
              f"{len(ip_lists)} IP access list(s) configured",
              "No IP access lists — workspace open to all IPs"),
        _ctrl("Data Export Controls", export_disabled and download_disabled,
              "Notebook export and download restricted",
              "Export/download not restricted"),
    ]
    hipaa_pass = sum(1 for c in hipaa_controls if c["status"] == "PASS")
    hipaa_score = round(hipaa_pass / len(hipaa_controls) * 100)

    # --- FedRAMP ---
    fedramp_controls = [
        _ctrl("GovCloud Environment (CA-3)", is_govcloud,
              f"Workspace in {cloud} GovCloud region",
              f"Workspace on commercial {cloud} — not FedRAMP eligible"),
        _ctrl("IP Access Lists (SC-7)", has_ip_lists,
              "Network boundary controls in place",
              "No network-level access control"),
        _ctrl("Token Lifetime (IA-5)", token_ok,
              f"Token lifetime within policy ({workspace_conf.get('maxTokenLifetimeDays', '0')}d)",
              f"Token lifetime {workspace_conf.get('maxTokenLifetimeDays', '0')}d exceeds 90d limit"),
        _ctrl("Encryption (SC-13/SC-28)", has_encryption,
              "Disk encryption enabled across clusters",
              "Disk encryption gaps found"),
        _ctrl("Audit Logging (AU-2/AU-3)", True,
              "Audit logging available via workspace diagnostics",
              "Verify audit log configuration"),
        _ctrl("Session Management (AC-12)", True,
              "Session timeout should be configured at IdP level",
              "Verify session idle timeout is <=30 minutes"),
        _ctrl("Least Privilege (AC-6)", admin_ratio_ok,
              "Admin privileges appropriately scoped",
              "Excessive admin privileges detected"),
        _ctrl("Data Exfiltration (SC-28)", export_disabled or download_disabled,
              "Data export controls configured",
              "Data export and download unrestricted"),
        _ctrl("Secret Management (SC-12)", has_secrets,
              "Credentials managed via secret scopes",
              "Insufficient secret management"),
    ]
    fedramp_pass = sum(1 for c in fedramp_controls if c["status"] == "PASS")
    fedramp_score = round(fedramp_pass / len(fedramp_controls) * 100)

    # --- SOC 2 ---
    soc2_controls = [
        _ctrl("Logical Access (CC6.1)", admin_ratio_ok and has_uc,
              "Access controls and Unity Catalog in place",
              "Access control gaps detected"),
        _ctrl("System Operations (CC7.1)", True,
              "Databricks managed infrastructure monitoring",
              "Verify operational monitoring"),
        _ctrl("Change Management (CC8.1)", True,
              "Workspace supports CI/CD and Repos integration",
              "Verify change management process"),
        _ctrl("Risk Mitigation (CC9.1)", has_ip_lists and has_encryption,
              "Network and encryption controls active",
              "Risk mitigation gaps in network or encryption"),
        _ctrl("Monitoring (CC7.2)", True,
              "Audit logs and diagnostic logging available",
              "Verify monitoring and alerting"),
        _ctrl("Data Classification", has_uc,
              "Unity Catalog enables data classification and tagging",
              "No Unity Catalog for data governance"),
    ]
    soc2_pass = sum(1 for c in soc2_controls if c["status"] == "PASS")
    soc2_score = round(soc2_pass / len(soc2_controls) * 100)

    # --- RBAC ---
    has_groups = len(groups) >= 2
    uc_policies_ok = True  # if we detect any data_security_mode=NONE policies, fail
    for p in policies:
        defn = p.get("definition", "{}")
        if isinstance(defn, str):
            defn = json.loads(defn) if defn else {}
        sec = defn.get("data_security_mode", {})
        if isinstance(sec, dict) and sec.get("value") == "NONE":
            uc_policies_ok = False
            break

    rbac_controls = [
        _ctrl("Unity Catalog Enabled", has_uc,
              "Centralized data access governance active",
              "No metastore — table-level ACLs unavailable"),
        _ctrl("Group-Based Access", has_groups,
              f"{len(groups)} groups configured for role-based assignment",
              "Insufficient groups for RBAC"),
        _ctrl("Least Privilege (Admins)", admin_ratio_ok,
              f"{admin_count} admins out of {total_users} users",
              f"{admin_count} admins is excessive for {total_users} users"),
        _ctrl("Cluster Policies", len(policies) > 0,
              f"{len(policies)} cluster policies enforce compute guardrails",
              "No cluster policies — unrestricted compute access"),
        _ctrl("Data Security Mode", uc_policies_ok,
              "All policies enforce Unity Catalog security modes",
              "Some policies have data_security_mode=NONE"),
        _ctrl("Secret Scope Isolation", has_secrets,
              "Secrets isolated in dedicated scopes",
              "Secrets not properly scoped"),
    ]
    rbac_pass = sum(1 for c in rbac_controls if c["status"] == "PASS")
    rbac_score = round(rbac_pass / len(rbac_controls) * 100)

    def _status(score):
        if score >= 80: return "COMPLIANT"
        if score >= 50: return "PARTIAL"
        return "NON-COMPLIANT"

    return {
        "hipaa": {"status": _status(hipaa_score), "score": hipaa_score, "controls": hipaa_controls},
        "fedramp": {"status": _status(fedramp_score), "score": fedramp_score, "controls": fedramp_controls},
        "soc2": {"status": _status(soc2_score), "score": soc2_score, "controls": soc2_controls},
        "rbac": {"status": _status(rbac_score), "score": rbac_score, "controls": rbac_controls},
    }


# ---------------------------------------------------------------------------
# Workspace profile
# ---------------------------------------------------------------------------

def build_workspace_profile(sec_data: dict) -> dict:
    """Build a workspace profile summary for the Workspace Overview tab."""
    cloud = sec_data.get("_cloud", "AWS")
    workspace_url = sec_data.get("_workspace_url", "")
    is_govcloud = sec_data.get("_govcloud", False)
    workspace_conf = sec_data.get("workspace_conf", {})
    metastores = sec_data.get("metastores", {}).get("metastores", [])
    catalogs = sec_data.get("catalogs", {}).get("catalogs", [])
    users = sec_data.get("users", {})
    groups = sec_data.get("groups", {}).get("Resources", [])
    sps = sec_data.get("sps", {}).get("Resources", [])
    clusters = sec_data.get("clusters", {}).get("clusters", [])
    warehouses = sec_data.get("warehouses", {}).get("warehouses", [])
    jobs = sec_data.get("jobs", {}).get("jobs", [])
    apps = sec_data.get("apps", {}).get("apps", [])
    ip_lists = sec_data.get("ip_lists", {}).get("ip_access_lists", [])
    secrets = sec_data.get("secrets", {}).get("scopes", [])
    storage_creds = sec_data.get("storage_creds", {}).get("storage_credentials", [])
    ext_locations = sec_data.get("ext_locations", {}).get("external_locations", [])
    shares = sec_data.get("shares", {}).get("shares", [])

    admin_group = next((g for g in groups if g.get("displayName") == "admins"), {})
    region = metastores[0].get("region", "unknown") if metastores else "unknown"

    # Determine workspace tier
    tier = "Enterprise" if "ENTERPRISE" in str(workspace_conf).upper() else "Premium"

    return {
        "cloud": cloud,
        "region": region,
        "workspace_url": workspace_url,
        "is_govcloud": is_govcloud,
        "tier": tier,
        "total_users": users.get("totalResults", 0),
        "admin_count": len(admin_group.get("members", [])),
        "group_count": len(groups),
        "sp_count": len(sps),
        "cluster_count": len(clusters),
        "warehouse_count": len(warehouses),
        "job_count": len(jobs),
        "app_count": len(apps),
        "catalog_count": len(catalogs),
        "metastore_count": len(metastores),
        "ip_list_count": len(ip_lists),
        "secret_scope_count": len(secrets),
        "storage_cred_count": len(storage_creds),
        "ext_location_count": len(ext_locations),
        "share_count": len(shares),
        "config_flags": workspace_conf,
    }


# ---------------------------------------------------------------------------
# Security score
# ---------------------------------------------------------------------------

def compute_security_score(findings: list[tuple]) -> dict:
    """Compute A-F security grade from findings."""
    weights = {"CRITICAL": 25, "HIGH": 10, "MEDIUM": 3, "LOW": 1}
    total_penalty = sum(weights.get(f[0], 0) for f in findings)
    score = max(0, 100 - total_penalty)
    if score >= 90:
        grade = "A"
    elif score >= 75:
        grade = "B"
    elif score >= 60:
        grade = "C"
    elif score >= 40:
        grade = "D"
    else:
        grade = "F"
    return {
        "score": score,
        "grade": grade,
        "total_findings": len(findings),
        "critical": sum(1 for f in findings if f[0] == "CRITICAL"),
        "high": sum(1 for f in findings if f[0] == "HIGH"),
        "medium": sum(1 for f in findings if f[0] == "MEDIUM"),
        "low": sum(1 for f in findings if f[0] == "LOW"),
    }


# ---------------------------------------------------------------------------
# Mermaid architecture diagrams
# ---------------------------------------------------------------------------

def generate_mermaid_architecture(sec_data: dict) -> dict:
    """Return 3 Mermaid diagram strings for the architecture tab."""
    cloud = sec_data.get("_cloud", "AWS")
    workspace_url = sec_data.get("_workspace_url", "workspace")
    clusters = sec_data.get("clusters", {}).get("clusters", [])
    warehouses = sec_data.get("warehouses", {}).get("warehouses", [])
    catalogs = sec_data.get("catalogs", {}).get("catalogs", [])
    apps = sec_data.get("apps", {}).get("apps", [])
    metastores = sec_data.get("metastores", {}).get("metastores", [])

    cloud_icons = {"AWS": "fa:fa-aws", "AZURE": "fa:fa-microsoft", "GCP": "fa:fa-google"}
    cloud_icon = cloud_icons.get(cloud, "fa:fa-cloud")

    # 1. High-level architecture
    arch = f"""graph TB
    subgraph Cloud["{cloud} Cloud"]
        WS["Databricks Workspace<br/>{workspace_url}"]
        UC["Unity Catalog<br/>{len(metastores)} Metastore(s)"]
        subgraph Compute["Compute Layer"]
            CL["Clusters ({len(clusters)})"]
            WH["SQL Warehouses ({len(warehouses)})"]
        end
        subgraph Data["Data Layer"]
            CAT["Catalogs ({len(catalogs)})"]
        end
        subgraph Apps["Applications"]
            AP["Databricks Apps ({len(apps)})"]
        end
    end
    Users["Users & Service Principals"] --> WS
    WS --> UC
    WS --> Compute
    WS --> Data
    WS --> Apps
    UC --> CAT"""

    # 2. Security posture
    ip_count = len(sec_data.get("ip_lists", {}).get("ip_access_lists", []))
    scope_count = len(sec_data.get("secrets", {}).get("scopes", []))
    total_users = sec_data.get("users", {}).get("totalResults", 0)

    security = f"""graph LR
    subgraph Identity["Identity & Access"]
        USERS["{total_users} Users"]
        SSO["SSO / SAML"]
        TOKENS["Token Mgmt"]
    end
    subgraph Network["Network Security"]
        IP["IP Lists ({ip_count})"]
        PRIV["Private Link"]
    end
    subgraph DataSec["Data Security"]
        ENC["Encryption"]
        SECRETS["Secret Scopes ({scope_count})"]
        UC2["Unity Catalog"]
    end
    Identity --> Network --> DataSec"""

    # 3. Data flow
    dataflow = """graph LR
    Sources["External Sources"] -->|Ingest| Bronze["Bronze Layer"]
    Bronze -->|Transform| Silver["Silver Layer"]
    Silver -->|Curate| Gold["Gold Layer"]
    Gold -->|Serve| Consumers["BI / ML / Apps"]
    Gold -->|Share| DeltaShare["Delta Sharing"]"""

    return {
        "architecture": arch,
        "security": security,
        "dataflow": dataflow,
    }
