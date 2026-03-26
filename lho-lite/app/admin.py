"""
Admin / setup page HTML generator for LHO Lite.

Blueprint dark theme. Two modes:
  - First-time wizard (stepped flow: Auth -> Test -> Schedule -> Save)
  - Returning user settings editor
"""

# ---------------------------------------------------------------------------
# Blueprint Logo SVG (inline)
# ---------------------------------------------------------------------------
LOGO_SVG = '<img src="/static/blueprint-logo.png" alt="Blueprint" style="height:32px">'


def render_admin_page(config: dict = None, is_setup: bool = False, message: str = "",
                      license_state: dict = None) -> str:
    """Return the full admin page HTML string."""
    config = config or {}
    license_state = license_state or {}

    license_key = config.get("license_key", "")
    license_valid = license_state.get("valid", False)
    license_expires = license_state.get("expires") or ""
    license_msg = license_state.get("message") or ""

    if license_valid:
        lic_status_html = f'<div style="margin-top:12px;padding:12px 16px;border-radius:8px;background:rgba(52,211,153,.1);border:1px solid rgba(52,211,153,.3);color:#34D399;font-size:13px"><i class="fas fa-check-circle"></i> License active{" — expires " + license_expires if license_expires else ""}</div>'
    elif license_key:
        lic_status_html = f'<div style="margin-top:12px;padding:12px 16px;border-radius:8px;background:rgba(248,113,113,.1);border:1px solid rgba(248,113,113,.3);color:#F87171;font-size:13px"><i class="fas fa-times-circle"></i> {license_msg or "License is invalid"}</div>'
    else:
        lic_status_html = ''

    workspace_url = config.get("workspace_url", "")
    auth_method = config.get("auth_method", "pat")
    pat_token = config.get("pat_token", "")
    sp_client_id = config.get("sp_client_id", "")
    sp_client_secret = config.get("sp_client_secret", "")
    sp_tenant_id = config.get("sp_tenant_id", "")
    refresh_schedule = config.get("refresh_schedule", "manual")
    refresh_hour = config.get("refresh_hour", "6")

    # Data destination settings
    data_dest = config.get("data_destination", "local")
    dest_catalog = config.get("dest_catalog", "")
    dest_schema = config.get("dest_schema", "")
    dest_table_prefix = config.get("dest_table_prefix", "lho_")
    lakebase_instance = config.get("lakebase_instance", "")
    lakebase_schema = config.get("lakebase_schema", "public")

    pat_checked = 'checked' if auth_method == "pat" else ""
    sp_checked = 'checked' if auth_method == "sp" else ""
    auto_checked = 'checked' if auth_method == "auto" else ""

    title = "Setup LHO Lite" if is_setup else "Settings"
    subtitle = "Configure your Databricks workspace connection" if is_setup else "Manage connection and schedule"

    msg_html = ""
    if message:
        msg_html = f'<div class="msg-box">{message}</div>'

    return f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>LHO Lite — {title}</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=Inter:wght@400;500;600&display=swap" rel="stylesheet">
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css">
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:#0D1117;color:#E6EDF3;font-family:'DM Sans','Inter',system-ui,sans-serif;font-size:14px;line-height:1.6;min-height:100vh;display:flex;align-items:center;justify-content:center}}
.container{{width:100%;max-width:640px;padding:40px 24px}}
.logo{{margin-bottom:32px;text-align:center}}
.logo svg{{height:32px}}
h1{{font-size:24px;font-weight:700;margin-bottom:4px;text-align:center}}
.subtitle{{color:#8B949E;font-size:14px;text-align:center;margin-bottom:32px}}
.card{{background:#161B22;border:1px solid #272D3F;border-radius:12px;padding:28px;margin-bottom:20px}}
.card-title{{font-size:13px;font-weight:600;color:#8B949E;text-transform:uppercase;letter-spacing:.5px;margin-bottom:16px;display:flex;align-items:center;gap:8px}}
.card-title i{{color:#4B7BF5}}
label{{display:block;font-size:13px;font-weight:500;color:#E6EDF3;margin-bottom:6px}}
.label-hint{{font-size:11px;color:#8B949E;font-weight:400;margin-left:4px}}
input[type="text"],input[type="password"],input[type="url"],input[type="number"],select{{
  width:100%;padding:10px 14px;background:#0D1117;border:1px solid #272D3F;border-radius:8px;color:#E6EDF3;font-size:14px;font-family:inherit;transition:border-color .15s;outline:none
}}
input:focus,select:focus{{border-color:#4B7BF5}}
input::placeholder{{color:#484F58}}
.radio-group{{display:flex;gap:12px;margin-bottom:16px}}
.radio-card{{flex:1;padding:14px;background:#0D1117;border:1px solid #272D3F;border-radius:8px;cursor:pointer;text-align:center;transition:all .15s}}
.radio-card:hover{{border-color:#4B7BF5}}
.radio-card.selected{{border-color:#4B7BF5;background:rgba(75,123,245,.08)}}
.radio-card input{{display:none}}
.radio-card .rc-icon{{font-size:20px;margin-bottom:4px;display:block}}
.radio-card .rc-label{{font-size:13px;font-weight:600}}
.radio-card .rc-hint{{font-size:11px;color:#8B949E}}
.field-group{{margin-bottom:16px}}
.sp-fields,.auto-fields{{display:none}}
.actions{{display:flex;gap:12px;margin-top:24px}}
.btn{{padding:12px 24px;border-radius:8px;font-size:14px;font-weight:600;cursor:pointer;border:none;transition:all .15s;font-family:inherit}}
.btn-primary{{background:#4B7BF5;color:#fff;flex:1}}
.btn-primary:hover{{background:#3D6BE0}}
.btn-secondary{{background:transparent;border:1px solid #272D3F;color:#E6EDF3}}
.btn-secondary:hover{{border-color:#4B7BF5;color:#4B7BF5}}
.btn-test{{background:transparent;border:1px solid #34D399;color:#34D399;width:100%}}
.btn-test:hover{{background:rgba(52,211,153,.08)}}
.btn-danger{{background:transparent;border:1px solid #F87171;color:#F87171}}
.btn-danger:hover{{background:rgba(248,113,113,.08)}}
#test-result{{margin-top:12px;padding:12px 16px;border-radius:8px;font-size:13px;display:none}}
#test-result.ok{{display:block;background:rgba(52,211,153,.1);border:1px solid rgba(52,211,153,.3);color:#34D399}}
#test-result.err{{display:block;background:rgba(248,113,113,.1);border:1px solid rgba(248,113,113,.3);color:#F87171}}
.msg-box{{background:rgba(75,123,245,.1);border:1px solid rgba(75,123,245,.3);color:#4B7BF5;padding:12px 16px;border-radius:8px;margin-bottom:20px;font-size:13px}}
.permissions{{background:#0D1117;border:1px solid #272D3F;border-radius:8px;padding:16px;margin-top:12px}}
.permissions h4{{font-size:12px;font-weight:600;color:#8B949E;text-transform:uppercase;margin-bottom:8px}}
.perm-list{{font-size:12px;color:#8B949E;line-height:1.8}}
.perm-list i{{color:#34D399;margin-right:6px;width:14px;text-align:center}}
.schedule-grid{{display:grid;grid-template-columns:1fr 1fr;gap:12px}}
.back-link{{display:inline-flex;align-items:center;gap:6px;color:#8B949E;text-decoration:none;font-size:13px;margin-bottom:16px}}
.back-link:hover{{color:#4B7BF5}}
.footer-text{{text-align:center;margin-top:32px;font-size:11px;color:#484F58}}
</style>
</head>
<body>
<div class="container">
  <div class="logo">{LOGO_SVG}</div>
  <h1>{title}</h1>
  <p class="subtitle">{subtitle}</p>
  {msg_html}
  {'<a href="/" class="back-link"><i class="fas fa-arrow-left"></i> Back to Dashboard</a>' if not is_setup else ''}

  <form id="admin-form" method="POST" action="/admin/save">

    <!-- License Key -->
    <div class="card">
      <div class="card-title"><i class="fas fa-certificate"></i> License</div>
      <div class="field-group">
        <label>License Key <span class="label-hint">(provided by Blueprint Technologies)</span></label>
        <input type="password" name="license_key" value="{license_key}" placeholder="BPT-XXXX-XXXX-XXXX-XXXX" autocomplete="off" id="license-key-input">
      </div>
      <button type="button" class="btn btn-test" id="license-btn" onclick="validateLicense()">
        <i class="fas fa-shield-halved"></i> Validate License
      </button>
      <div id="license-result">{lic_status_html}</div>
    </div>

    <!-- Auth Method -->
    <div class="card">
      <div class="card-title"><i class="fas fa-key"></i> Authentication</div>
      <div class="radio-group">
        <label class="radio-card {'selected' if auth_method=='pat' else ''}" id="rc-pat">
          <input type="radio" name="auth_method" value="pat" {pat_checked}>
          <span class="rc-icon"><i class="fas fa-ticket"></i></span>
          <span class="rc-label">PAT Token</span>
          <span class="rc-hint">Personal Access Token</span>
        </label>
        <label class="radio-card {'selected' if auth_method=='sp' else ''}" id="rc-sp">
          <input type="radio" name="auth_method" value="sp" {sp_checked}>
          <span class="rc-icon"><i class="fas fa-id-badge"></i></span>
          <span class="rc-label">Service Principal</span>
          <span class="rc-hint">OAuth2 Client Credentials</span>
        </label>
        <label class="radio-card {'selected' if auth_method=='auto' else ''}" id="rc-auto">
          <input type="radio" name="auth_method" value="auto" {auto_checked}>
          <span class="rc-icon"><i class="fas fa-wand-magic-sparkles"></i></span>
          <span class="rc-label">Auto (SDK)</span>
          <span class="rc-hint">Databricks App Runtime</span>
        </label>
      </div>

      <div class="field-group">
        <label>Workspace URL</label>
        <input type="url" name="workspace_url" value="{workspace_url}" placeholder="https://adb-123.4.azuredatabricks.net">
      </div>

      <!-- PAT fields -->
      <div class="pat-fields" style="{'display:block' if auth_method=='pat' else 'display:none'}">
        <div class="field-group">
          <label>Personal Access Token <span class="label-hint">(dapi...)</span></label>
          <input type="password" name="pat_token" value="{pat_token}" placeholder="dapi1234567890abcdef" autocomplete="off">
        </div>
      </div>

      <!-- SP fields -->
      <div class="sp-fields" style="{'display:block' if auth_method=='sp' else 'display:none'}">
        <div class="field-group">
          <label>Client ID <span class="label-hint">(Application ID)</span></label>
          <input type="text" name="sp_client_id" value="{sp_client_id}" placeholder="00000000-0000-0000-0000-000000000000">
        </div>
        <div class="field-group">
          <label>Client Secret</label>
          <input type="password" name="sp_client_secret" value="{sp_client_secret}" placeholder="Secret value" autocomplete="off">
        </div>
        <div class="field-group">
          <label>Tenant ID <span class="label-hint">(Azure only, leave blank for AWS/GCP)</span></label>
          <input type="text" name="sp_tenant_id" value="{sp_tenant_id}" placeholder="Azure AD tenant ID">
        </div>
      </div>

      <!-- Auto fields -->
      <div class="auto-fields" style="{'display:block' if auth_method=='auto' else 'display:none'}">
        <div class="permissions">
          <h4>Auto-detection</h4>
          <p style="font-size:12px;color:#8B949E">Uses Databricks SDK default auth chain. Works when running as a Databricks App or with environment variables (DATABRICKS_HOST, DATABRICKS_TOKEN).</p>
        </div>
      </div>

      <button type="button" class="btn btn-test" id="test-btn" onclick="testConnection()">
        <i class="fas fa-plug"></i> Test Connection
      </button>
      <div id="test-result"></div>
    </div>

    <!-- Required Permissions -->
    <div class="card">
      <div class="card-title"><i class="fas fa-shield-halved"></i> Required Permissions</div>
      <div class="permissions">
        <h4>REST API Access</h4>
        <div class="perm-list">
          <div><i class="fas fa-check"></i> Workspace admin OR account admin (for full SCIM data)</div>
          <div><i class="fas fa-check"></i> CAN_MANAGE on clusters (for cluster list)</div>
          <div><i class="fas fa-check"></i> CAN_USE on at least one SQL warehouse</div>
          <div><i class="fas fa-check"></i> USE CATALOG on system catalog (for billing/query history)</div>
          <div><i class="fas fa-check"></i> Unity Catalog metastore admin (for full catalog listing)</div>
        </div>
      </div>
      <div class="permissions" style="margin-top:12px">
        <h4>Cloud-Specific Notes</h4>
        <div class="perm-list">
          <div><i class="fas fa-cloud"></i> <strong>AWS:</strong> OIDC token endpoint for SP auth; system.billing.list_prices available on E2</div>
          <div><i class="fab fa-microsoft"></i> <strong>Azure:</strong> SP needs Azure AD app registration; use Tenant ID for OAuth2</div>
          <div><i class="fab fa-google"></i> <strong>GCP:</strong> SP auth via OIDC; some system tables may have limited availability</div>
        </div>
      </div>
    </div>

    <!-- Schedule -->
    <div class="card">
      <div class="card-title"><i class="fas fa-clock"></i> Refresh Schedule</div>
      <div class="schedule-grid">
        <div class="field-group">
          <label>Frequency</label>
          <select name="refresh_schedule">
            <option value="manual" {'selected' if refresh_schedule=='manual' else ''}>Manual only</option>
            <option value="hourly" {'selected' if refresh_schedule=='hourly' else ''}>Every hour</option>
            <option value="daily" {'selected' if refresh_schedule=='daily' else ''}>Daily</option>
            <option value="weekly" {'selected' if refresh_schedule=='weekly' else ''}>Weekly</option>
          </select>
        </div>
        <div class="field-group">
          <label>Hour (UTC) <span class="label-hint">for daily/weekly</span></label>
          <input type="number" name="refresh_hour" value="{refresh_hour}" min="0" max="23" step="1">
        </div>
      </div>
    </div>

    <!-- Data Destination -->
    <div class="card">
      <div class="card-title"><i class="fas fa-database"></i> Data Destination</div>
      <p style="font-size:12px;color:#8B949E;margin-bottom:16px">Choose where collected data is stored after each refresh. Local stores in the app's SQLite DB. Delta and Lakebase persist data externally for querying.</p>
      <div class="radio-group">
        <label class="radio-card {'selected' if data_dest=='local' else ''}" id="rc-local">
          <input type="radio" name="data_destination" value="local" {'checked' if data_dest=='local' else ''}>
          <span class="rc-icon"><i class="fas fa-hard-drive"></i></span>
          <span class="rc-label">Local Only</span>
          <span class="rc-hint">App SQLite DB</span>
        </label>
        <label class="radio-card {'selected' if data_dest=='delta' else ''}" id="rc-delta">
          <input type="radio" name="data_destination" value="delta" {'checked' if data_dest=='delta' else ''}>
          <span class="rc-icon"><i class="fas fa-layer-group"></i></span>
          <span class="rc-label">Delta Table</span>
          <span class="rc-hint">Unity Catalog</span>
        </label>
        <label class="radio-card {'selected' if data_dest=='lakebase' else ''}" id="rc-lakebase">
          <input type="radio" name="data_destination" value="lakebase" {'checked' if data_dest=='lakebase' else ''}>
          <span class="rc-icon"><i class="fas fa-cubes"></i></span>
          <span class="rc-label">Lakebase</span>
          <span class="rc-hint">PostgreSQL-compatible</span>
        </label>
        <label class="radio-card {'selected' if data_dest=='both' else ''}" id="rc-both">
          <input type="radio" name="data_destination" value="both" {'checked' if data_dest=='both' else ''}>
          <span class="rc-icon"><i class="fas fa-arrows-split-up-and-left"></i></span>
          <span class="rc-label">Both</span>
          <span class="rc-hint">Delta + Lakebase</span>
        </label>
      </div>

      <!-- Delta Table fields -->
      <div class="delta-fields" style="{'display:block' if data_dest in ('delta','both') else 'display:none'}">
        <div style="background:#0D1117;border:1px solid #272D3F;border-radius:8px;padding:16px;margin-bottom:12px">
          <h4 style="font-size:12px;font-weight:600;color:#4B7BF5;text-transform:uppercase;margin-bottom:12px"><i class="fas fa-layer-group"></i> Delta Table Settings</h4>
          <div class="schedule-grid">
            <div class="field-group">
              <label>Catalog</label>
              <input type="text" name="dest_catalog" value="{dest_catalog}" placeholder="lho_data">
            </div>
            <div class="field-group">
              <label>Schema</label>
              <input type="text" name="dest_schema" value="{dest_schema}" placeholder="lho_lite">
            </div>
          </div>
          <div class="field-group">
            <label>Table Prefix <span class="label-hint">(tables: &lt;prefix&gt;security, &lt;prefix&gt;usage, etc.)</span></label>
            <input type="text" name="dest_table_prefix" value="{dest_table_prefix}" placeholder="lho_">
          </div>
        </div>
      </div>

      <!-- Lakebase fields -->
      <div class="lakebase-fields" style="{'display:block' if data_dest in ('lakebase','both') else 'display:none'}">
        <div style="background:#0D1117;border:1px solid #272D3F;border-radius:8px;padding:16px">
          <h4 style="font-size:12px;font-weight:600;color:#34D399;text-transform:uppercase;margin-bottom:12px"><i class="fas fa-cubes"></i> Lakebase Settings</h4>
          <div class="schedule-grid">
            <div class="field-group">
              <label>Instance Name</label>
              <input type="text" name="lakebase_instance" value="{lakebase_instance}" placeholder="my-lakebase">
            </div>
            <div class="field-group">
              <label>Schema</label>
              <input type="text" name="lakebase_schema" value="{lakebase_schema}" placeholder="public">
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- Actions -->
    <div class="actions">
      <button type="submit" class="btn btn-primary">
        <i class="fas fa-save"></i> {'Save & Start' if is_setup else 'Save Settings'}
      </button>
      {'<button type="button" class="btn btn-danger" onclick="if(confirm(&apos;Reset all config?&apos;)) window.location.href=&apos;/admin/reset&apos;">Reset</button>' if not is_setup else ''}
    </div>
  </form>

  <p class="footer-text">LHO Lite v1.0 &mdash; Lakehouse Optimizer Lite by Blueprint</p>
</div>

<script>
// Radio card selection — auth method
document.querySelectorAll('.radio-card input[name="auth_method"]').forEach(r => {{
  r.addEventListener('change', () => {{
    r.closest('.radio-group').querySelectorAll('.radio-card').forEach(c => c.classList.remove('selected'));
    r.closest('.radio-card').classList.add('selected');
    document.querySelector('.pat-fields').style.display = r.value === 'pat' ? 'block' : 'none';
    document.querySelector('.sp-fields').style.display = r.value === 'sp' ? 'block' : 'none';
    document.querySelector('.auto-fields').style.display = r.value === 'auto' ? 'block' : 'none';
  }});
}});

// Radio card selection — data destination
document.querySelectorAll('.radio-card input[name="data_destination"]').forEach(r => {{
  r.addEventListener('change', () => {{
    r.closest('.radio-group').querySelectorAll('.radio-card').forEach(c => c.classList.remove('selected'));
    r.closest('.radio-card').classList.add('selected');
    const v = r.value;
    document.querySelector('.delta-fields').style.display = (v === 'delta' || v === 'both') ? 'block' : 'none';
    document.querySelector('.lakebase-fields').style.display = (v === 'lakebase' || v === 'both') ? 'block' : 'none';
  }});
}});

// Test connection
async function testConnection() {{
  const btn = document.getElementById('test-btn');
  const res = document.getElementById('test-result');
  btn.disabled = true;
  btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Testing...';
  res.style.display = 'none';

  const form = new FormData(document.getElementById('admin-form'));
  try {{
    const resp = await fetch('/admin/test', {{method: 'POST', body: form}});
    const data = await resp.json();
    if (data.ok) {{
      res.className = 'ok';
      res.innerHTML = '<i class="fas fa-check-circle"></i> Connected as <strong>' + data.user + '</strong> (' + data.cloud + ')';
    }} else {{
      res.className = 'err';
      res.innerHTML = '<i class="fas fa-times-circle"></i> ' + (data.error || 'Connection failed');
    }}
    res.style.display = 'block';
  }} catch (e) {{
    res.className = 'err';
    res.innerHTML = '<i class="fas fa-times-circle"></i> Network error: ' + e.message;
    res.style.display = 'block';
  }}
  btn.disabled = false;
  btn.innerHTML = '<i class="fas fa-plug"></i> Test Connection';
}}

// Validate license
async function validateLicense() {{
  const btn = document.getElementById('license-btn');
  const res = document.getElementById('license-result');
  const key = document.getElementById('license-key-input').value.trim();
  if (!key) {{
    res.innerHTML = '<div style="margin-top:12px;padding:12px 16px;border-radius:8px;background:rgba(248,113,113,.1);border:1px solid rgba(248,113,113,.3);color:#F87171;font-size:13px"><i class="fas fa-times-circle"></i> Please enter a license key</div>';
    return;
  }}
  btn.disabled = true;
  btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Validating...';
  try {{
    const resp = await fetch('/admin/validate-license', {{
      method: 'POST',
      headers: {{'Content-Type': 'application/json'}},
      body: JSON.stringify({{license_key: key}})
    }});
    const data = await resp.json();
    if (data.valid) {{
      res.innerHTML = '<div style="margin-top:12px;padding:12px 16px;border-radius:8px;background:rgba(52,211,153,.1);border:1px solid rgba(52,211,153,.3);color:#34D399;font-size:13px"><i class="fas fa-check-circle"></i> License valid' + (data.expires ? ' — expires ' + data.expires : '') + '</div>';
    }} else {{
      res.innerHTML = '<div style="margin-top:12px;padding:12px 16px;border-radius:8px;background:rgba(248,113,113,.1);border:1px solid rgba(248,113,113,.3);color:#F87171;font-size:13px"><i class="fas fa-times-circle"></i> ' + (data.message || 'License is invalid') + '</div>';
    }}
  }} catch (e) {{
    res.innerHTML = '<div style="margin-top:12px;padding:12px 16px;border-radius:8px;background:rgba(248,113,113,.1);border:1px solid rgba(248,113,113,.3);color:#F87171;font-size:13px"><i class="fas fa-times-circle"></i> Error: ' + e.message + '</div>';
  }}
  btn.disabled = false;
  btn.innerHTML = '<i class="fas fa-shield-halved"></i> Validate License';
}}
</script>
</body>
</html>'''
