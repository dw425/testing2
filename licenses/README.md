# LHO Lite — License Registry

This directory contains the license registry that the LHO Lite app validates against.

## How it works

1. The app fetches `licenses.json` from this repo via GitHub raw content URL
2. It looks up the customer's license key in the `licenses` object
3. If the key exists, is marked `valid: true`, and hasn't expired → app runs normally
4. Between remote checks (every 30 days), the app validates locally against the stored expiration date

## Managing licenses

### Issue a new license

Add an entry to `licenses.json`:

```json
"LHO-CUST-0001-ACME": {
    "valid": true,
    "expires": "2027-01-01",
    "customer": "Acme Corp",
    "message": "Licensed to Acme Corp"
}
```

### Revoke a license

Set `valid` to `false`:

```json
"LHO-CUST-0001-ACME": {
    "valid": false,
    "expires": "2027-01-01",
    "customer": "Acme Corp",
    "message": "License revoked — contact support@blueprint.tech"
}
```

The customer's app will stop working on the next remote check (within 30 days), or immediately if they re-validate from the admin page.

### Rotate a license key

Add a `new_key` field to the old entry — the app will automatically swap to the new key:

```json
"LHO-CUST-0001-ACME": {
    "valid": true,
    "expires": "2027-01-01",
    "customer": "Acme Corp",
    "message": "Licensed to Acme Corp",
    "new_key": "LHO-CUST-0002-ACME"
}
```

And add the new key entry as well.

### Extend a license

Update the `expires` date. The app picks it up on the next 30-day check.

## Key format convention

```
LHO-{TYPE}-{SEQ}-{CUSTOMER}
```

- **TYPE**: `DEMO`, `TRIAL`, `PROD`, `ENT` (enterprise)
- **SEQ**: 4-digit sequence number
- **CUSTOMER**: Short customer identifier

## Security

- If this repo is private, the app needs a GitHub token (set `LHO_GITHUB_TOKEN` env var or store as `github_token` in the app config)
- License keys are stored encrypted at rest in the customer's app (Fernet encryption in SQLite)
