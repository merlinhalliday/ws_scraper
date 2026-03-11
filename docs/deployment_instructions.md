# Deployment Instructions

## Purpose

This document deploys the worker pipeline in two parts:

1. Cloud workers in Azure Container Apps:
   - `ingest`
   - `persist`
   - `transform`
   - `export-dispatch`
2. A local-only relay process that consumes staged export intents and uploads them to a personal OneDrive folder.

The cloud path is fully unattended. The personal OneDrive hop is intentionally local because personal Microsoft accounts do not fit the app-only cloud auth model used by the rest of the platform.

## Prerequisites

- Azure subscription with permission to create resource groups, Container Apps, Service Bus, Storage, ACR, and managed identities.
- Azure CLI installed and logged in.
- Docker is not required locally if you use `az acr build`.
- Python 3.11 or newer for the local relay.
- A personal Microsoft account that already has access to the target OneDrive.

Install or update the Azure Container Apps extension first:

```powershell
az login
az account set --subscription "<subscription-id>"
az extension add --name containerapp --upgrade
az provider register --namespace Microsoft.App
az provider register --namespace Microsoft.OperationalInsights
az provider register --namespace Microsoft.ServiceBus
az provider register --namespace Microsoft.Storage
az provider register --namespace Microsoft.ContainerRegistry
```

## Step 1: Set Deployment Variables

Use a dedicated PowerShell session and set all names up front.

```powershell
$Location = "westeurope"
$ResourceGroup = "ws-scraper-rg"
$Workspace = "ws-scraper-law"
$ContainerEnv = "ws-scraper-env"
$Registry = "wsscraperacr001"
$Storage = "wsscraperstor001"
$ServiceBus = "ws-scraper-sb"
$IdentityName = "ws-scraper-uami"
$Tag = (Get-Date -Format "yyyyMMddHHmmss")
$Image = "$Registry.azurecr.io/ws-scraper:$Tag"

$RawContainer = "ws-snapshots-raw"
$SilverContainer = "ws-snapshots-silver"
$ExportContainer = "ws-snapshots-export"
$LedgerContainer = "ws-snapshots-ledger"

$IngestQueue = "ws-snapshot-batches"
$TransformQueue = "ws-bronze-written"
$ExportQueue = "ws-export-intents"
$RelayQueue = "ws-relay-intents"

$BlobAccountUrl = "https://$Storage.blob.core.windows.net"
$ServiceBusFqdn = "$ServiceBus.servicebus.windows.net"
```

## Step 2: Create Core Azure Resources

Create the resource group, Log Analytics workspace, Container Apps environment, registry, storage account, Service Bus namespace, and user-assigned managed identity.

```powershell
az group create -n $ResourceGroup -l $Location

az monitor log-analytics workspace create `
  -g $ResourceGroup `
  -n $Workspace `
  -l $Location

az containerapp env create `
  -g $ResourceGroup `
  -n $ContainerEnv `
  -l $Location `
  --logs-workspace-id (az monitor log-analytics workspace show -g $ResourceGroup -n $Workspace --query customerId -o tsv) `
  --logs-workspace-key (az monitor log-analytics workspace get-shared-keys -g $ResourceGroup -n $Workspace --query primarySharedKey -o tsv)

az acr create `
  -g $ResourceGroup `
  -n $Registry `
  -l $Location `
  --sku Basic

az storage account create `
  -g $ResourceGroup `
  -n $Storage `
  -l $Location `
  --sku Standard_LRS `
  --kind StorageV2 `
  --allow-blob-public-access false

az servicebus namespace create `
  -g $ResourceGroup `
  -n $ServiceBus `
  -l $Location `
  --sku Standard

az identity create `
  -g $ResourceGroup `
  -n $IdentityName `
  -l $Location
```

Capture the identity values for later steps:

```powershell
$IdentityId = az identity show -g $ResourceGroup -n $IdentityName --query id -o tsv
$IdentityPrincipalId = az identity show -g $ResourceGroup -n $IdentityName --query principalId -o tsv
$IdentityClientId = az identity show -g $ResourceGroup -n $IdentityName --query clientId -o tsv
$StorageId = az storage account show -g $ResourceGroup -n $Storage --query id -o tsv
$ServiceBusId = az servicebus namespace show -g $ResourceGroup -n $ServiceBus --query id -o tsv
$RegistryId = az acr show -g $ResourceGroup -n $Registry --query id -o tsv
```

## Step 3: Create Blob Containers And Service Bus Queues

Create the four Blob containers and the four queues. Enable duplicate detection on the queues so deterministic message IDs are useful during retries and restarts.

```powershell
az storage container create --account-name $Storage --name $RawContainer --auth-mode login
az storage container create --account-name $Storage --name $SilverContainer --auth-mode login
az storage container create --account-name $Storage --name $ExportContainer --auth-mode login
az storage container create --account-name $Storage --name $LedgerContainer --auth-mode login

az servicebus queue create -g $ResourceGroup --namespace-name $ServiceBus --name $IngestQueue --enable-duplicate-detection true
az servicebus queue create -g $ResourceGroup --namespace-name $ServiceBus --name $TransformQueue --enable-duplicate-detection true
az servicebus queue create -g $ResourceGroup --namespace-name $ServiceBus --name $ExportQueue --enable-duplicate-detection true
az servicebus queue create -g $ResourceGroup --namespace-name $ServiceBus --name $RelayQueue --enable-duplicate-detection true
```

## Step 4: Grant Managed Identity Access

Grant one shared user-assigned identity permission to pull images, read and write Blob data, and send and receive Service Bus messages.

```powershell
az role assignment create --assignee-object-id $IdentityPrincipalId --role "AcrPull" --scope $RegistryId
az role assignment create --assignee-object-id $IdentityPrincipalId --role "Storage Blob Data Contributor" --scope $StorageId
az role assignment create --assignee-object-id $IdentityPrincipalId --role "Azure Service Bus Data Sender" --scope $ServiceBusId
az role assignment create --assignee-object-id $IdentityPrincipalId --role "Azure Service Bus Data Receiver" --scope $ServiceBusId
```

Wait 2 to 5 minutes after the role assignments so Azure RBAC has time to propagate before you deploy the apps.

## Step 5: Build And Push The Image

Build directly in Azure Container Registry:

```powershell
az acr build `
  -r $Registry `
  -t "ws-scraper:$Tag" `
  .
```

## Step 6: Define Shared Environment Variables

All cloud workers use the same shared baseline. The only value that changes per app is the startup command and the queue scale target.

```powershell
$CommonEnv = @(
  "APP_ENV=cloud",
  "IDENTITY_MODE=managed_identity",
  "EXPORT_MODE=blob_relay",
  "QUEUE_BACKEND=servicebus",
  "AZURE_CLIENT_ID=$IdentityClientId",
  "AZURE_SERVICEBUS_FQDN=$ServiceBusFqdn",
  "AZURE_BLOB_ACCOUNT_URL=$BlobAccountUrl",
  "AZURE_BLOB_RAW_CONTAINER=$RawContainer",
  "AZURE_BLOB_SILVER_CONTAINER=$SilverContainer",
  "AZURE_BLOB_EXPORT_CONTAINER=$ExportContainer",
  "AZURE_BLOB_LEDGER_CONTAINER=$LedgerContainer",
  "INGEST_QUEUE_NAME=$IngestQueue",
  "TRANSFORM_QUEUE_NAME=$TransformQueue",
  "EXPORT_QUEUE_NAME=$ExportQueue",
  "RELAY_QUEUE_NAME=$RelayQueue",
  "OBS_HOST=0.0.0.0",
  "OBS_PORT=8080",
  "LOG_LEVEL=INFO",
  "WORKER_HEARTBEAT_TIMEOUT_SECONDS=120"
)
```

## Step 7: Create The Container Apps

Create four apps with the same image and the shared user-assigned identity. Use command override to select the worker entrypoint.

Create the ingest app first. It should stay at one replica because it owns the live websocket connections.

```powershell
az containerapp create `
  -g $ResourceGroup `
  -n "ws-ingest" `
  --environment $ContainerEnv `
  --image $Image `
  --registry-server "$Registry.azurecr.io" `
  --registry-identity $IdentityId `
  --user-assigned $IdentityId `
  --ingress internal `
  --target-port 8080 `
  --min-replicas 1 `
  --max-replicas 1 `
  --cpu 0.5 `
  --memory 1.0Gi `
  --command python `
  --args "-m" "app.ingest_worker" `
  --env-vars $CommonEnv
```

Create the persist app and scale it from the ingest queue:

```powershell
az containerapp create `
  -g $ResourceGroup `
  -n "ws-persist" `
  --environment $ContainerEnv `
  --image $Image `
  --registry-server "$Registry.azurecr.io" `
  --registry-identity $IdentityId `
  --user-assigned $IdentityId `
  --ingress internal `
  --target-port 8080 `
  --min-replicas 1 `
  --max-replicas 5 `
  --cpu 0.5 `
  --memory 1.0Gi `
  --command python `
  --args "-m" "app.persist_worker" `
  --env-vars $CommonEnv `
  --scale-rule-name "ingest-queue" `
  --scale-rule-type "azure-servicebus" `
  --scale-rule-metadata "queueName=$IngestQueue" "namespace=$ServiceBusFqdn" "messageCount=10" `
  --scale-rule-identity $IdentityId
```

Create the transform app and scale it from the transform queue:

```powershell
az containerapp create `
  -g $ResourceGroup `
  -n "ws-transform" `
  --environment $ContainerEnv `
  --image $Image `
  --registry-server "$Registry.azurecr.io" `
  --registry-identity $IdentityId `
  --user-assigned $IdentityId `
  --ingress internal `
  --target-port 8080 `
  --min-replicas 1 `
  --max-replicas 5 `
  --cpu 0.5 `
  --memory 1.0Gi `
  --command python `
  --args "-m" "app.transform_worker" `
  --env-vars $CommonEnv `
  --scale-rule-name "transform-queue" `
  --scale-rule-type "azure-servicebus" `
  --scale-rule-metadata "queueName=$TransformQueue" "namespace=$ServiceBusFqdn" "messageCount=10" `
  --scale-rule-identity $IdentityId
```

Create the export-dispatch app and scale it from the export queue:

```powershell
az containerapp create `
  -g $ResourceGroup `
  -n "ws-export" `
  --environment $ContainerEnv `
  --image $Image `
  --registry-server "$Registry.azurecr.io" `
  --registry-identity $IdentityId `
  --user-assigned $IdentityId `
  --ingress internal `
  --target-port 8080 `
  --min-replicas 1 `
  --max-replicas 3 `
  --cpu 0.5 `
  --memory 1.0Gi `
  --command python `
  --args "-m" "app.export_worker" `
  --env-vars $CommonEnv `
  --scale-rule-name "export-queue" `
  --scale-rule-type "azure-servicebus" `
  --scale-rule-metadata "queueName=$ExportQueue" "namespace=$ServiceBusFqdn" "messageCount=10" `
  --scale-rule-identity $IdentityId
```

## Step 8: Configure Health Probes

For each Container App, configure these probes against port `8080`:

- Startup probe:
  - Path: `/healthz`
  - Initial delay: `20`
  - Period: `10`
  - Failure threshold: `12`
- Liveness probe:
  - Path: `/healthz`
  - Initial delay: `30`
  - Period: `30`
  - Failure threshold: `3`
- Readiness probe:
  - Path: `/readyz`
  - Initial delay: `10`
  - Period: `15`
  - Failure threshold: `3`

If you manage ACA through YAML or ARM, put these probes on every worker container. If you manage the apps manually, open each Container App in the Azure portal and add the probes in the revision template.

## Step 9: Verify Cloud Deployment

Check revision status:

```powershell
az containerapp show -g $ResourceGroup -n ws-ingest --query "properties.runningStatus"
az containerapp show -g $ResourceGroup -n ws-persist --query "properties.runningStatus"
az containerapp show -g $ResourceGroup -n ws-transform --query "properties.runningStatus"
az containerapp show -g $ResourceGroup -n ws-export --query "properties.runningStatus"
```

Check logs for worker startup:

```powershell
az containerapp logs show -g $ResourceGroup -n ws-ingest --tail 100
az containerapp logs show -g $ResourceGroup -n ws-persist --tail 100
az containerapp logs show -g $ResourceGroup -n ws-transform --tail 100
az containerapp logs show -g $ResourceGroup -n ws-export --tail 100
```

Verify the probe endpoints from inside the containers:

```powershell
az containerapp exec -g $ResourceGroup -n ws-ingest --command "sh -c 'wget -qO- http://127.0.0.1:8080/readyz'"
az containerapp exec -g $ResourceGroup -n ws-persist --command "sh -c 'wget -qO- http://127.0.0.1:8080/readyz'"
az containerapp exec -g $ResourceGroup -n ws-transform --command "sh -c 'wget -qO- http://127.0.0.1:8080/readyz'"
az containerapp exec -g $ResourceGroup -n ws-export --command "sh -c 'wget -qO- http://127.0.0.1:8080/readyz'"
```

## Step 10: Validate Bronze, Silver, And Export Staging

After a few minutes of live runtime, validate the pipeline in order:

1. Check that the ingest queue is not stuck:

```powershell
az servicebus queue show -g $ResourceGroup --namespace-name $ServiceBus --name $IngestQueue --query "countDetails.activeMessageCount"
```

2. Check that Bronze blobs are being written:

```powershell
az storage blob list --account-name $Storage --container-name $RawContainer --auth-mode login --num-results 10 --output table
```

3. Check that Silver Parquet files exist:

```powershell
az storage blob list --account-name $Storage --container-name $SilverContainer --auth-mode login --num-results 10 --output table
```

4. Check that staged export payloads and manifests exist:

```powershell
az storage blob list --account-name $Storage --container-name $ExportContainer --auth-mode login --prefix "relay/" --num-results 20 --output table
```

5. Inspect one manifest and confirm the target path looks correct:

```powershell
az storage blob download `
  --account-name $Storage `
  --container-name $ExportContainer `
  --name "relay/manifests/<export-id>.json" `
  --file ".\relay-manifest.json" `
  --auth-mode login
Get-Content .\relay-manifest.json
```

## Step 11: Run The Local OneDrive Relay

The local relay is the only step that uses delegated device-code auth. Run it on a workstation you control.

Set local relay environment variables:

```powershell
$env:APP_ENV = "local"
$env:IDENTITY_MODE = "delegated_local"
$env:EXPORT_MODE = "personal_onedrive_local"
$env:QUEUE_BACKEND = "servicebus"
$env:AZURE_SERVICEBUS_CONNECTION_STRING = "<service-bus-connection-string>"
$env:AZURE_BLOB_CONNECTION_STRING = "<storage-connection-string>"
$env:AZURE_BLOB_RAW_CONTAINER = $RawContainer
$env:AZURE_BLOB_SILVER_CONTAINER = $SilverContainer
$env:AZURE_BLOB_EXPORT_CONTAINER = $ExportContainer
$env:AZURE_BLOB_LEDGER_CONTAINER = $LedgerContainer
$env:INGEST_QUEUE_NAME = $IngestQueue
$env:TRANSFORM_QUEUE_NAME = $TransformQueue
$env:EXPORT_QUEUE_NAME = $ExportQueue
$env:RELAY_QUEUE_NAME = $RelayQueue
$env:GRAPH_CLIENT_ID = "<entra-app-client-id-for-consumer-auth>"
$env:GRAPH_AUTHORITY = "https://login.microsoftonline.com/consumers"
$env:GRAPH_SCOPES = "Files.ReadWrite"
$env:ONEDRIVE_FOLDER = "/ws-snapshots"
```

Start the relay:

```powershell
python -m app.local_relay_worker
```

On first run, the worker logs the device-code prompt. Complete that sign-in from a browser. The cache is then stored in `.graph_token_cache.bin`, and later runs reuse it silently unless the refresh token expires or is revoked.

## Step 12: Test The Full Flow

### Smoke Test

1. Let all four cloud workers run for at least 5 minutes.
2. Confirm Bronze, Silver, and staged export blobs exist.
3. Start the local relay.
4. Confirm a new Parquet file appears in the target personal OneDrive folder.

### Restart Test

1. Restart `ws-persist`, `ws-transform`, and `ws-export` one at a time.
2. Confirm processing resumes without manual repair.
3. Check that the ledger container contains `persist/`, `transform/`, `export/`, and `relay/` records for processed items.

### Backlog Test

1. Stop `ws-export` for 5 minutes.
2. Confirm the export queue grows while Bronze and Silver continue.
3. Start `ws-export` again.
4. Confirm the export queue drains and new staged blobs appear.

### Relay Test

1. Stop the local relay for 10 minutes while cloud workers continue.
2. Confirm the relay queue grows.
3. Start the relay again.
4. Confirm the queue drains and OneDrive catches up.

## Alert Thresholds

Use these as the initial alert thresholds:

- No data:
  - Alert if no new Bronze blob appears for 5 minutes during market hours.
- Backlog growth:
  - Alert if any of the ingest, transform, export, or relay queues stays above 500 active messages for 10 minutes.
- Export failures:
  - Alert if `export_failures_total` increases 3 times within 15 minutes.
- Auth failures:
  - Alert immediately on any repeated Graph or managed identity auth error.
- Health:
  - Alert if any worker stays not-ready for longer than 3 probe periods or restarts more than 3 times in 15 minutes.

## Failure Modes And Recovery

### Ingest Stops Producing Data

1. Inspect `ws-ingest` logs.
2. Confirm the websocket connections are still active.
3. Restart the app if it is wedged:

```powershell
az containerapp revision restart -g $ResourceGroup -n ws-ingest --revision (az containerapp revision list -g $ResourceGroup -n ws-ingest --query "[?properties.active].name" -o tsv)
```

### Persist Or Transform Queue Grows Without Draining

1. Check the target worker logs for repeated exceptions.
2. Verify the managed identity still has Blob and Service Bus access.
3. Increase `maxReplicas` temporarily if the worker is healthy but under-scaled.
4. If the worker is unhealthy, restart it and re-check queue depth after 5 minutes.

### Export Dispatch Succeeds But Personal OneDrive Is Behind

1. Confirm new files are landing under the export container `relay/payloads/`.
2. Check the relay queue depth.
3. Start or restart the local relay worker.
4. If Graph auth has expired, delete `.graph_token_cache.bin` and run the relay again to force a new device-code sign-in.

### Duplicate Or Conflicting Outputs

1. Check the ledger container first. Duplicate work should be bounded by the `persist/`, `transform/`, `export/`, and `relay/` ledger records.
2. If a specific item is genuinely corrupted, delete only the single affected ledger blob and requeue or replay the corresponding message.
3. Do not bulk-delete ledger blobs or queue contents unless you are intentionally replaying the entire pipeline.

## Official References

- Azure Identity for Python: https://learn.microsoft.com/en-us/azure/developer/python/sdk/authentication/overview
- Container Apps managed identity for ACR image pulls: https://learn.microsoft.com/en-us/azure/container-apps/managed-identity-image-pull
- Container Apps scale rules: https://learn.microsoft.com/en-us/azure/container-apps/scale-app
- Container Apps health probes: https://learn.microsoft.com/en-us/azure/container-apps/health-probes
- Service Bus duplicate detection: https://learn.microsoft.com/en-us/azure/service-bus-messaging/duplicate-detection
- Graph drive endpoint restrictions: https://learn.microsoft.com/en-us/graph/api/drive-get
- Graph upload sessions: https://learn.microsoft.com/en-us/graph/api/driveitem-createuploadsession
