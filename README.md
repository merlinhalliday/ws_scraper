# ws_scraper

Simple Polymarket + Coinbase websocket data siphon with optional OneDrive upload through Microsoft Graph.

## Azure-hosted Graph auth (app-only)

`ws_scraper.py` now uses **non-interactive app-only Graph authentication** for runtime uploads.

Supported runtime auth modes:

- **Azure AD app registration + client secret**
- **Azure AD app registration + certificate credential**
- **Managed identity** (`GRAPH_USE_MANAGED_IDENTITY=true`), including user-assigned identity via `GRAPH_MANAGED_IDENTITY_CLIENT_ID`

### Required environment variables

Common:

- `GRAPH_UPLOAD_ENABLED=true`
- `GRAPH_SCOPES=https://graph.microsoft.com/.default`
- `ONEDRIVE_FOLDER=/path/in/drive`
- `GRAPH_ONEDRIVE_TARGET=me` (OneDrive for the app owner) or `sites` (SharePoint/OneDrive for Business site-backed drive)

Client secret mode:

- `GRAPH_CLIENT_ID=<app-registration-client-id>`
- `GRAPH_AUTHORITY=https://login.microsoftonline.com/<tenant-id>`
- `GRAPH_CLIENT_SECRET=<secret>`

Certificate mode:

- `GRAPH_CLIENT_ID=<app-registration-client-id>`
- `GRAPH_AUTHORITY=https://login.microsoftonline.com/<tenant-id>`
- `GRAPH_CLIENT_CERTIFICATE_PATH=/path/to/private_key.pem`
- Optional: `GRAPH_CLIENT_CERTIFICATE_PASSWORD=<passphrase>`
- Optional: `GRAPH_CLIENT_CERTIFICATE_THUMBPRINT=<hex-thumbprint>`

Managed identity mode:

- `GRAPH_USE_MANAGED_IDENTITY=true`
- Optional: `GRAPH_MANAGED_IDENTITY_CLIENT_ID=<user-assigned-mi-client-id>`

## Local bootstrap-only delegated login

Device code flow is intentionally isolated to local tooling and is **not used by cloud runtime**.

Use:

```bash
python tools/local_auth_bootstrap.py --env-file .env
```

This is useful for local bootstrap/troubleshooting only.

## Microsoft Graph app permissions for OneDrive uploads

For app-only runtime uploads, configure **Application permissions** on Microsoft Graph and grant **admin consent**:

- `Files.ReadWrite.All` (application)
- `Sites.ReadWrite.All` (application)

Notes:

- Upload APIs invoked by this project require tenant-admin consent for application permissions.
- Without admin consent, token acquisition may succeed but upload calls will fail with authorization errors.
- If writing to SharePoint-backed drives (`GRAPH_ONEDRIVE_TARGET=sites`), `Sites.ReadWrite.All` is typically required.

## Quick checks

```bash
ls -lh
```
