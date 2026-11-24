# Backrest Operator — Usage Guide

This guide covers installing the Helm chart and creating the core Custom Resources (CRs) for `operator.backrest.io/v1alpha1`.

## Prerequisites

- Kubernetes 1.27+ with a CSI snapshot controller (for snapshot-based backups)
- Helm 3
- A restic-compatible object store or filesystem backend
- Optional: [TopoLVM](https://github.com/topolvm/topolvm) when using `topolvmSnapshot` in the backup pipeline

Default Backrest image pin: `ghcr.io/garethgeorge/backrest:v1.14.1`

## Install the operator

```bash
helm upgrade --install backrest-operator ./charts/backrest-operator \
  --namespace backrest-system \
  --create-namespace
```

Verify the operator and MCP pods are running:

```bash
kubectl get pods -n backrest-system
kubectl get crd | grep operator.backrest.io
```

### Watch filters (optional)

Limit which namespaces the operator reconciles by setting `operator.watch.namespaces` in Helm values:

```yaml
operator:
  watch:
    namespaces:
      - backrest-system
      - app-prod
```

An empty list watches all namespaces.

## 1. Create a BackrestCluster

Deploy the Backrest host, agents (multihost mode), and optional UI ingress:

```bash
kubectl apply -f examples/backrestcluster.yaml
kubectl get backrestcluster -n backrest-system
```

The host keeps its data (oplog, `jwt-secret`) on a PVC, but not its config file: a restarted host starts with Backrest's default config (auth disabled, no repositories). The operator notices the host Deployment change and writes instance, auth and the missing repositories back. Agents connect to the host Service on port `9898`.

### Backrest API authentication

Backrest checks HTTP Basic auth (or a UI login JWT) on its API once `auth` is enabled in its config. The operator owns that block:

1. Create the Secret in the BackrestCluster namespace, without a trailing newline in the values:

   ```bash
   kubectl -n backrest-system create secret generic backrest-api-auth \
     --from-literal=username=operator --from-literal=password="$(openssl rand -base64 33)"
   ```

2. Let the MCP server use it: Helm value `backrestAuth.existingSecret: backrest-api-auth`, then upgrade the chart. The operator reads the Secret itself.
3. Set `spec.auth.existingSecret: backrest-api-auth` and `spec.auth.enabled: true` on the BackrestCluster. On every sync the operator writes `auth.users` (this one user, password as base64 bcrypt) and sends Basic auth on its own calls; users added by hand in the UI are removed. With `enabled: false` it writes `auth.disabled: true`.

Not affected: multihost agent sync (`/v1sync.BackrestSyncService`, key-based) and signed `/download/` links. Affected: the UI, `/metrics` on the host, and every other `/v1.Backrest/*` caller, which now needs the credentials. A UI behind an auth proxy must inject `Authorization: Basic ...` itself, or users log in with the Backrest user.

See [examples/backrestcluster.yaml](../examples/backrestcluster.yaml).

## 2. Create a BackupRepository

Define where restic stores snapshots. Credentials live in Kubernetes Secrets — never inline in the CR.

```bash
kubectl apply -f examples/auth-secret.yaml
kubectl apply -f examples/backuprepository-s3.yaml
kubectl get backuprepository -n backrest-system
```

### Append-only repositories

`spec.appendOnly: true` is accepted on the CR. **Enforcement on forget/prune Jobs and MCP is not wired yet** — treat as a declaration until P2.

### Scheduled `restic check` (verify)

By default, `spec.verify.enabled` is `true`. The operator owns a CronJob that runs `restic check` on the cadence in `spec.verify.schedule` (default: weekly). Disabling verify deletes that CronJob.

```yaml
spec:
  verify:
    enabled: false
```

See [examples/backuprepository-s3.yaml](../examples/backuprepository-s3.yaml).

## 3. BackupPlan (stub)

`BackupPlan` currently only writes fragments into `ConfigMap/backrest-plans`. The Backrest host does not mount them yet. **Use `PVCBackup` for real scheduled backups.**

```bash
kubectl apply -f examples/backupplan.yaml
kubectl get backupplan -n app
```

See [examples/backupplan.yaml](../examples/backupplan.yaml).

## 4. Back up a PVC

Create a `PVCBackup` CR. The operator owns Jobs and reconciles `spec.schedule` in-process (annotate `operator.backrest.io/force-run` for an immediate run). Do not hand-roll CronJobs or ServiceAccounts for this.

### CSI snapshot pipeline

Requires a `VolumeSnapshotClass` and CSI snapshot support:

```bash
kubectl apply -f examples/pvcbackup-csi.yaml
kubectl get pvcbackup -n app -w
```

### Quiesced live copy

Scale workloads down (or delete pods) before copying files:

```bash
kubectl apply -f examples/pvcbackup-quiesced.yaml
```

**Important:** `spec.quiesce.leaveDown: true` keeps workloads stopped after backup. This requires the confirmation annotation `operator.backrest.io/leave-down-confirmed: "true"` on the CR metadata.

### Pipeline steps

`spec.strategy.pipeline` is an ordered list. Supported values:

| Step | Description |
|------|-------------|
| `liveFlush` | Run flush command/script before backup |
| `csiSnapshot` | Create a CSI VolumeSnapshot |
| `topolvmSnapshot` | TopoLVM-specific snapshot (requires TopoLVM) |
| `quiescedLive` | Quiesce workloads and copy live files |

You can combine steps (for example, snapshot then upload from the snapshot mount).

## 5. Restore a PVC

### Restore to an existing PVC

```bash
kubectl apply -f examples/pvcrestore-existing.yaml
kubectl get pvcrestore -n app -w
```

### Download a snapshot as `.tar` (Backrest API — preferred)

Backrest streams a snapshot (or a path inside it) as a tar archive via
**GetDownloadURL**. The signed JWT in the URL is the credential (download Ingress
bypasses oauth2-proxy).

#### Option A — `SnapshotDownload` CR (URL in status)

```bash
kubectl apply -f examples/snapshotdownload.yaml
kubectl get snapdl -n app -w
# when Phase=Ready:
kubectl get snapdl -n app download-app-data-latest -o jsonpath='{.status.downloadURL}{"\n"}'
curl -L -o snapshot.tar "$(kubectl get snapdl -n app download-app-data-latest -o jsonpath='{.status.downloadURL}')"
```

Remint after expiry:

```bash
kubectl annotate snapdl -n app download-app-data-latest operator.backrest.io/refresh="$(date +%s)" --overwrite
```

#### Option B — MCP (immediate or via CR)

- `get_snapshot_download_url` — mint immediately, returns `downloadURL`
- `create_snapshot_download` — creates the CR and waits until `status.downloadURL` is Ready
- `get_snapshot_download` — read CR status

#### Option C — kubectl into host pod

```bash
# 1) Find the indexed-snapshot operation for your restic snapshot id
HOST_POD=$(kubectl get pod -n backrest -l app.kubernetes.io/component=host -o jsonpath='{.items[0].metadata.name}')
SNAP_ID='<full-restic-snapshot-id>'

kubectl exec -n backrest "$HOST_POD" -- wget -qO- \
  --post-data="{\"selector\":{\"snapshotId\":\"$SNAP_ID\"},\"lastN\":5}" \
  --header='Content-Type: application/json' \
  http://127.0.0.1:9898/v1.Backrest/GetOperations
# Note the operation "id" where operationIndexSnapshot is set (e.g. "11").

# 2) Mint a signed relative download URL (path "/" = whole snapshot as .tar)
OP_ID=11
kubectl exec -n backrest "$HOST_POD" -- wget -qO- \
  --post-data="{\"opId\":$OP_ID,\"filePath\":\"/\"}" \
  --header='Content-Type: application/json' \
  http://127.0.0.1:9898/v1.Backrest/GetDownloadURL
# → {"value":"./download/<jwt>/"}

# 3) Download via the public UI host (operator download Ingress)
curl -L -o snapshot.tar "https://<ingress-host>/download/<jwt>/"
```

Optional: set `path` to a subdirectory (e.g. `/data/...`) to dump only that tree.

Disable the bypass Ingress if needed:

```yaml
spec:
  host:
    ingress:
      downloadBypass:
        enabled: false
```

## 6. RBAC for human users

Bind users or groups to shipped ClusterRoles:

| Role | ClusterRole name | Capabilities |
|------|------------------|--------------|
| Read-only | `backrest-viewer` | get/list/watch CRs and status |
| Operator | `backrest-operator` | create/update backups, plans, restores |
| Admin | `backrest-admin` | delete, secrets, destructive operations |

See [examples/rbac-binding.yaml](../examples/rbac-binding.yaml).

## 7. External Secrets (optional)

If you use [External Secrets Operator](https://external-secrets.io/), sync repository credentials into the Secret referenced by `passwordSecretRef`. See the commented example in [examples/externalsecret.yaml](../examples/externalsecret.yaml).

## Troubleshooting

```bash
# Operator logs
kubectl logs -n backrest-system deploy/backrest-operator -f

# PVCBackup status
kubectl describe pvcbackup -n app <name>

# Webhook rejections
kubectl get validatingwebhookconfiguration
```

## Related docs

- [MCP server usage](./MCP.md)
- [Manual backup without operator](./MANUAL_BACKUP.md)
- [Manual restore without operator](./MANUAL_RESTORE.md)
- [Operator disaster recovery](./OPERATOR_DR.md)
