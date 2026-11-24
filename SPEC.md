# Backrest Operator — Specification

**Project:** Backrest Operator  
**API group:** `operator.backrest.io`  
**API version:** `v1alpha1`  
**License:** Apache-2.0  
**Language:** English (code, docs, comments, examples)  
**Upstream backup manager:** [garethgeorge/backrest](https://github.com/garethgeorge/backrest) (Web UI + orchestrator for [restic](https://restic.net/))  
**Stack:** Go (controller-runtime) operator + Go MCP server (separate Deployment)  
**Backrest image:** pin to the **latest stable** upstream release at each chart/operator release  

This document is the single source of truth for implementing and evolving the project. Do not embed organization-specific infrastructure names, hostnames, buckets, or credentials in the repository.

### Operator model (non-negotiable)

Users declare **Custom Resources only**. The operator owns derived objects (Deployments, DaemonSets, Services, Ingresses, Jobs, CronJobs for repository verify, mirrored Secrets). Do **not** ship hand-rolled ServiceAccounts, Roles, CronJobs, or backup scripts in consumer overlays for features the CRDs already cover.

### Implementation status (honest)

| Area | Status |
|------|--------|
| `BackrestCluster` host + agents + Ingress (annotations / TLS / backend Service) | **Implemented** |
| Agent `nodeSelector` + `tolerations` | **Implemented** |
| `BackupRepository` + verify CronJob (`restic check`) | **Implemented** |
| `PVCBackup` quiescedLive, multi-PVC (`pvcNames`), schedule (in-process cron), retention keepLast, nodeSelector | **Implemented** |
| `PVCBackup` CSI/TopoLVM snapshot (single PVC), wait ReadyToUse | **Implemented** |
| `PVCBackup` `liveFlush` / flush exec | **Not implemented** (webhook rejects `liveFlush`) |
| `PVCBackup` quiesce `deletePods` | **Not implemented** (scaleToZero only) |
| `BackupPlan` → live Backrest plan sync | **Stub** (writes `ConfigMap/backrest-plans` fragments; host does not mount/apply them yet) |
| `BackupRepository.backrest.syncToHost` | **Not wired** |
| `appendOnly` enforcement | **Field only** (not enforced on Jobs/MCP yet) |
| `auth.enabled` / `auth.existingSecret` | **Implemented** (Basic auth user written into the host config on every sync; operator and MCP clients authenticate) |
| `PVCRestore` existing/new | **Partial** (Jobs created; success may be reported before Job completes) |
| Validating webhooks | **Implemented** (pvcName \| pvcNames) |
| MCP TokenReview / SAR (tools run as the MCP SA, no impersonation) | **Implemented** (tool catalog subset of §6.6) |
| Monitoring embeds (ServiceMonitor / VM* / Grafana) | **Implemented** (chart toggles) |

---

## 1. Goals

| ID | Goal | Outcome |
|----|------|---------|
| G1 | Kubernetes-native backup operator | CRDs drive Backrest + restic + CSI snapshots with a single cluster-scoped operator and watch filters |
| G2 | Agent-operable MCP | Separate MCP Deployment lets AI agents create/delete/trigger plans and manage backups under Kubernetes RBAC |
| G3 | Flexible backup modes | Live flush, quiesced file copy, CSI/TopoLVM snapshots, and ordered combinations |
| G4 | Backrest feature parity | Cron, on-demand, retention/forget, prune, hooks/bash scripts — everything Backrest already does |
| G5 | Restore paths | Existing PVC, new PVC, partial paths, local download via Backrest GetDownloadURL |
| G6 | Observability | Prometheus + VictoriaMetrics scrapes, embedded alert rules, Grafana dashboard |
| G7 | Opensource-ready | Generic Helm charts, docs, Docker build, unit tests; no private infra coupling |
| G8 | Operator DR | Backup of operator/Backrest state + manual runbooks when the operator is unavailable |

### Non-goals (v1)

- Cross-cluster disaster-recovery orchestration
- Replacing the Backrest Web UI for day-2 browsing
- Kind/k3d e2e in CI (deferred; unit tests + image/chart CI only)
- Vendoring Backrest GPL sources into the operator image (call upstream binary/API only)
- Organization-specific examples (internal domains, buckets, node labels)

---

## 2. Architecture

```
                    ┌─────────────────────────────────────────────┐
                    │              AI / human clients             │
                    │   Cursor agents · curl · kubectl · UI       │
                    └───────────────┬─────────────┬───────────────┘
                                    │             │
                    Bearer kube token│             │ Ingress (opt auth)
                                    ▼             ▼
                         ┌────────────────┐  ┌──────────────┐
                         │  backrest-mcp  │  │ Backrest UI  │
                         │  Deployment    │  │ host :9898   │
                         │  HTTP/SSE+stdio│  └──────┬───────┘
                         └───────┬────────┘         │ multihost
                                 │ TokenReview      ▼
                                 │ SAR+Impersonate ┌──────────────┐
                                 ▼                 │ agents (DS)  │
                         ┌────────────────┐        └──────────────┘
                         │ Kubernetes API │
                         │ + Validating   │
                         │   Webhooks     │
                         └───────┬────────┘
                                 │ CRs (operator.backrest.io)
                                 ▼
                         ┌────────────────┐
                         │ backrest-      │  watch filters
                         │ operator       │  (ns / labels)
                         └───────┬────────┘
              ┌──────────────────┼──────────────────┐
              ▼                  ▼                  ▼
     VolumeSnapshot      restic Jobs         GetDownloadURL (/download)
     + quiesce/flush     → any restic        (TTL HTTP / curl)
                         backend
```

### Components

| Component | Role |
|-----------|------|
| **backrest-operator** | Reconciles CRs; deploys Backrest host + agents; runs snapshot/quiesce/restic Jobs; serves `/metrics`; registers validating webhooks |
| **backrest-mcp** | Separate Deployment (process isolation). MCP tools over Streamable HTTP/SSE and stdio. Does not share the operator process |
| **Backrest host + agents** | Upstream Backrest in server–agent (multihost) mode; Web UI via Service/Ingress; optional UI auth |
| **GetDownloadURL** | Native Backrest signed URL streaming a snapshot path as `.tar` via host `/download` Ingress |

### Watch model

- One **cluster-scoped** operator Deployment.
- Configurable **filters**: namespace allow/deny lists and/or label selectors on watched CRs and related workloads.
- Default: watch all namespaces (empty allow-list = all).

---

## 3. Custom Resources (`operator.backrest.io/v1alpha1`)

### 3.1 `BackrestCluster`

Deploys and wires Backrest host + agents.

```yaml
apiVersion: operator.backrest.io/v1alpha1
kind: BackrestCluster
metadata:
  name: main
  namespace: backrest-system
spec:
  version: ""                          # empty = chart default = latest stable pin
  image: ghcr.io/garethgeorge/backrest
  host:
    replicas: 1
    serviceType: ClusterIP
    enableServiceLinks: false          # avoid K8s BACKREST_PORT injection clash
    ingress:
      enabled: false
      className: ""
      host: backrest.example.com
      annotations: {}                  # cert-manager, external-dns, Traefik, …
      backendServiceName: ""           # empty = Backrest host Service; set to oauth2-proxy when used
      backendServicePort: 0            # 0 = Backrest port 9898
      tls: []
    persistence:
      size: 20Gi
      storageClassName: ""
    resources: {}
    nodeSelector: {}
    tolerations: []
  agents:
    enabled: true
    mode: DaemonSet                    # DaemonSet | Deployment
    replicas: 1                        # Deployment only
    nodeSelector: {}
    tolerations: []
    multihost:
      serverURL: ""                    # default http://<host-svc>.<ns>.svc:9898
      permissions:
        - ReadOperations
        - ReceiveSharedRepos
  auth:
    enabled: false
    existingSecret: ""                 # Secret keys: username, password
  monitoring:
    enabled: true
  mcp:
    enabled: true                      # deploy companion MCP from same chart or subchart
status:
  phase: Pending                       # Pending | Ready | Degraded | Failed
  hostReady: false
  agentsReady: 0
  agentsDesired: 0
  multihostPaired: 0
  conditions: []
```

### 3.2 `BackupRepository`

Any restic/Backrest-supported backend.

```yaml
apiVersion: operator.backrest.io/v1alpha1
kind: BackupRepository
metadata:
  name: primary
  namespace: backrest-system
spec:
  # Full restic repository URL (s3:, b2:, azure:, gs:, sftp:, rclone:, local path, etc.)
  url: "s3:https://s3.example.com/my-bucket/backups"
  passwordSecretRef:
    name: backup-repo
    key: RESTIC_PASSWORD
  # Optional backend env (AWS_*, B2_*, AZURE_*, rclone config, etc.)
  envFromSecretRef:
    name: backup-repo-env
  appendOnly: false                    # when true: restic append-only policy / flags
  verify:
    enabled: true                      # default ON; set false to disable
    schedule: "0 3 * * 0"              # restic check cadence
  shared: true                         # mark shared in Backrest multihost when applicable
  backrest:
    syncToHost: true
    clusterRef:
      name: main
      namespace: backrest-system
status:
  phase: Pending
  resticURL: ""
  lastCheckTime: ""
  lastCheckResult: ""
  conditions: []
```

**Secrets:** Kubernetes `Secret` is required for passwords/keys. Optional integrations (documented examples, not hard dependencies): External Secrets Operator, HashiCorp Vault Agent/CSI.

### 3.3 `BackupPlan`

Schedules and retention intended for Backrest parity (cron, hooks, forget policy).

> **Current behavior:** the reconciler writes plan fragments into `ConfigMap/backrest-plans`. The Backrest host Deployment does **not** yet mount or apply those fragments. Prefer `PVCBackup` for Kubernetes-orchestrated backups until plan sync is implemented.

```yaml
apiVersion: operator.backrest.io/v1alpha1
kind: BackupPlan
metadata:
  name: app-data-daily
  namespace: app
spec:
  repositoryRef:
    name: primary
    namespace: backrest-system
  clusterRef:
    name: main
    namespace: backrest-system
  schedule: "0 2 * * *"                # empty = manual / on-demand only
  paths: ["/data"]
  excludes: ["**/lost+found"]
  retention:
    keepLast: 7
    keepDaily: 7
    keepWeekly: 4
    keepMonthly: 6
    keepYearly: 0
  hooks:                               # Backrest/bash hooks (future sync)
    - condition: CONDITION_SNAPSHOT_START
      action: shell
      command: |
        #!/bin/bash
        set -euo pipefail
        echo "pre-backup"
  tags:
    - k8s
  pvcBackupRef:
    name: ""
    namespace: ""
status:
  phase: Pending
  lastBackupTime: ""
  lastSnapshotID: ""
  conditions: []
```

### 3.4 `PVCBackup`

PVC-centric backup with strategy pipeline. **This is the primary day-2 backup CR.**

```yaml
apiVersion: operator.backrest.io/v1alpha1
kind: PVCBackup
metadata:
  name: app-pvc
  namespace: app
spec:
  # One of pvcName or pvcNames (multi-volume, one quiesce window)
  pvcName: app-data
  # pvcNames: ["app-data-0", "app-data-1"]
  repositoryRef:
    name: primary
    namespace: backrest-system
  strategy:
    pipeline:
      - quiescedLive                   # default when pipeline empty
      # - csiSnapshot                  # single PVC only
      # - topolvmSnapshot
      # - liveFlush                    # not implemented (admission rejects)
  volumeSnapshotClassName: ""          # required for snapshot steps
  flush:
    enabled: false
    mode: exec
    targetPod: {}
    script: ""
  quiesce:
    enabled: false
    timeoutSeconds: 900
    leaveDown: false                   # MUST default false; always unquiesce in finally unless true
    targets:
      - apiVersion: apps/v1
        kind: StatefulSet
        name: app
        namespace: ""                   # empty = PVCBackup namespace
        # action: scaleToZero          # deletePods not implemented
  paths: []                            # empty = /data/<pvcName> mounts; do not default to "/"
  excludes: []
  schedule: ""                         # cron; reconciled in-process (no per-CR CronJob)
  nodeSelector: {}                     # restic Job (required for RWO/local volumes)
  retention:
    keepLast: 6
    deleteVolumeSnapshotAfterUpload: true
  backoffLimit: 2
  ttlSecondsAfterFinished: 86400
status:
  phase: Pending
  # Pending|Scheduled|Quiescing|Snapshotting|Uploading|Succeeded|Failed
  lastBackupTime: ""
  lastSnapshotName: ""
  lastResticSnapshotID: ""
  lastJobName: ""
  lastDurationSeconds: 0
  lastForceRun: ""                     # last consumed operator.backrest.io/force-run value
  conditions: []
```

On-demand trigger while scheduled: annotate  
`operator.backrest.io/force-run=<unique-token>`.

#### Strategy semantics

| Step | Behavior |
|------|----------|
| `quiescedLive` | Scale targets → mount source PVC(s) RO → restic → unquiesce |
| `csiSnapshot` | VolumeSnapshot → wait ReadyToUse → clone PVC → restic (single PVC) |
| `topolvmSnapshot` | Same as CSI with TopoLVM class; pin Job via `nodeSelector` when needed |
| `liveFlush` | **Not implemented** |

Combinations are an ordered `pipeline`. Empty pipeline defaults to `quiescedLive`.

### 3.5 `PVCRestore`

```yaml
apiVersion: operator.backrest.io/v1alpha1
kind: PVCRestore
metadata:
  name: app-restore
  namespace: app
spec:
  mode: fromResticToExistingPVC
  # fromVolumeSnapshot | fromResticToNewPVC | fromResticToExistingPVC
  repositoryRef:
    name: primary
    namespace: backrest-system
  restic:
    snapshotID: latest
    pathFilters: []                    # partial restore; empty = full
  volumeSnapshotRef:
    name: ""
    namespace: ""
  target:
    existingPVCName: app-data          # for existing PVC mode
    newPVC:
      name: app-data-restored
      size: 100Gi
      storageClassName: ""
      accessModes: ["ReadWriteOnce"]
  quiesce:
    enabled: true
    timeoutSeconds: 900
    targets: []
status:
  phase: Pending
  lastJobName: ""
  conditions: []
```

---

## 4. Backup algorithms

### 4.1 CSI / TopoLVM snapshot → restic

1. Optional `liveFlush`.
2. Optional quiesce (if requested in pipeline).
3. Create `VolumeSnapshot` with configured `VolumeSnapshotClass`.
4. Wait until `ReadyToUse`.
5. Create temporary PVC from snapshot (`dataSource`) — RO mount in Job.
6. For TopoLVM: schedule Job on the snapshot source node when the driver requires it.
7. Run `restic backup` into `BackupRepository` URL with password/env from Secrets.
8. Apply retention (Backrest forget and/or operator cleanup of VolumeSnapshots).
9. Always unquiesce in `finally` unless `leaveDown: true`.
10. Update status + metrics.

### 4.2 Quiesced live file copy

1. Scale/stop targets holding the PVC.
2. Mount source PVC in Job (RWO: app must be down).
3. `restic backup` → unquiesce → status/metrics.

### 4.3 Scheduling and retention

- **`PVCBackup.spec.schedule`:** operator reconciles cron **in-process** (requeue until due). No per-backup CronJob or trigger ServiceAccount. Force with annotation `operator.backrest.io/force-run`.
- **`BackupRepository.spec.verify`:** operator-owned CronJob runs `restic check` (deleted when verify disabled).
- **`BackupPlan`:** not a runtime scheduler until Backrest plan sync lands; use `PVCBackup` for K8s quiesce/snapshot orchestration.
- Retention: `PVCBackup.spec.retention.keepLast` → `restic forget --keep-last --prune` after backup.

### 4.4 Append-only

When `BackupRepository.spec.appendOnly: true`, the operator/Backrest configure the repository for append-only restic usage (deny forget/prune/delete that would rewrite history, per restic append-only semantics). Destructive MCP tools that would violate append-only must fail closed unless an admin overrides with explicit flags **and** RBAC admin role.

---

## 5. Restore algorithms

### 5.1 Existing PVC

1. Optional quiesce of workloads using the PVC.
2. Job mounts PVC; `restic restore` with optional `pathFilters`.
3. Unquiesce; update status.

### 5.2 New PVC

- From VolumeSnapshot: PVC with `dataSource` = snapshot.
- From restic: create empty PVC → Job mounts → `restic restore`.

### 5.3 Snapshot download (Backrest GetDownloadURL)

1. Index the repository so Backrest has an `operationIndexSnapshot` for the restic snapshot id.
2. Call Backrest `GetDownloadURL` (MCP `get_snapshot_download_url`) with `opId` + path (`/` = whole snapshot as `.tar`).
3. Download via the host download Ingress: `https://<ui-host>/download/<jwt>/` (bypasses oauth2-proxy; JWT is the credential).

---

## 6. MCP server

### 6.1 Deployment

- Separate Deployment `backrest-mcp`, Service, optional Ingress.
- Own ServiceAccount, resource limits, and scrape config.
- Must not run inside the operator process.

### 6.2 Transports

- **Streamable HTTP / SSE** for remote/in-cluster agents.
- **stdio** for local IDE/CLI (inherits caller kubeconfig).

### 6.3 Authentication and authorization (Kubernetes-native)

Per-user identity — do **not** execute tools solely as the MCP pod SA.

**HTTP/SSE flow:**

1. Client sends `Authorization: Bearer <token>` (user OIDC/exec plugin token, or `kubectl create token`).
2. MCP validates with **TokenReview** (MCP SA: `create` on `tokenreviews.authentication.k8s.io`).
3. Each tool maps to Kubernetes resource attributes on CRs (and related objects).
4. **SubjectAccessReview** filters `list_tools` and gates `call_tool`.
5. API calls run as the MCP ServiceAccount after the SubjectAccessReview passed (no impersonation: an unrestricted `impersonate` rule is cluster-admin).

**stdio:** use the caller’s kubeconfig; apply the same SAR rules where feasible.

**MCP pod SA permissions (minimal):** TokenReview, SubjectAccessReview, Impersonate users/groups — **not** blanket CR admin.

### 6.4 RBAC roles (shipped)

| ClusterRole | Capabilities |
|-------------|--------------|
| `backrest-viewer` | get/list/watch CRs; read backup metadata/status |
| `backrest-operator` | create/update plans & backups; trigger backups; restores |
| `backrest-admin` | delete; destructive flags; repository secret refs; append-only overrides |

### 6.5 Destructive operations

Tools that delete plans, snapshots, repositories, or force wipe require:

```text
allow_destructive: true
```

Default is `false`. Webhooks and MCP both enforce this.

### 6.6 Tool catalog (v1)

| Tool | Verb mapping | Notes |
|------|--------------|-------|
| `list_clusters` / `get_cluster` | get/list BackrestCluster | |
| `list_repositories` / `get_repository` | get/list BackupRepository | |
| `create_repository` / `update_repository` | create/update | |
| `delete_repository` | delete | requires `allow_destructive` |
| `list_plans` / `get_plan` | get/list BackupPlan | |
| `create_plan` / `update_plan` | create/update | |
| `delete_plan` | delete | requires `allow_destructive` |
| `trigger_backup` | create Job / annotate plan | on-demand backup |
| `list_snapshots` / `get_snapshot` | get via status/Backrest/restic | |
| `delete_snapshot` | forget/delete | requires `allow_destructive`; blocked if appendOnly |
| `create_pvc_backup` / `get_pvc_backup` | PVCBackup CRUD | |
| `create_pvc_restore` / `get_pvc_restore` | PVCRestore CRUD | pathFilters supported |
| `get_snapshot_download_url` | signed Backrest download URL for a snapshot path |
| `repo_status` | get + metrics summary | storage usage when available |

---

## 7. Validating webhooks

Admission webhooks validate CRs and provide guardrails for AI/MCP-generated objects.

**Must reject or require extra flags for:**

- Missing `repositoryRef` / invalid restic URL scheme
- Snapshot strategy without `volumeSnapshotClassName`
- Quiesce `leaveDown: true` without admin annotation/flag
- Destructive deletes without `allow_destructive` equivalent annotation on the CR (when created via MCP)
- Append-only repo with prune/forget policies that contradict append-only
- Cross-namespace refs outside operator watch filters (when filters enabled)

Webhook TLS: cert-manager (preferred) or operator-managed certificates.

---

## 8. Backrest host mode

- Run Backrest in **server–agent (multihost)** mode: one host Deployment + agent DaemonSet (or Deployment).
- Web UI exposed via Service; optional Ingress.
- Optional authentication for the UI (Backrest-supported auth via Secret).
- Set `enableServiceLinks: false` (or explicitly set `BACKREST_PORT`) to avoid Kubernetes service env clashing with Backrest’s `BACKREST_PORT`.
- Default image tag: latest stable upstream at release time.

---

## 9. Monitoring and alerting

### 9.1 Metrics (Prometheus text)

Expose at least:

- Backup success/failure counts and last status (per plan/PVC)
- Backup duration histograms/summaries
- Snapshot/copy counts retained
- Repository storage usage and fill ratio (when backend/API allows; otherwise restic stats best-effort)
- Operator reconcile errors; MCP auth denials

### 9.2 Helm embeds (all toggleable)

| Resource | Default |
|----------|---------|
| `ServiceMonitor` | on when `monitoring.serviceMonitor.enabled` |
| `VMServiceScrape` | on when `monitoring.vmServiceScrape.enabled` |
| `PrometheusRule` | on when `monitoring.prometheusRules.enabled` |
| `VMRule` | on when `monitoring.vmRules.enabled` |
| Grafana dashboard ConfigMap | on when `monitoring.grafanaDashboard.enabled` |

### 9.3 Alert examples (embedded)

- Backup failed / stuck in Failed
- Backup not succeeding within SLA window
- Repository storage above warning threshold (configurable, e.g. 80%)
- Repository storage above critical threshold (e.g. 95%)
- Operator / MCP / Backrest target down

No organization-specific alert names or receivers in the chart defaults.

---

## 10. Helm chart surface

Chart name: `backrest-operator`.

Suggested values (non-exhaustive):

```yaml
operator:
  image:
    repository: ghcr.io/reactive-network/backrest-operator
    tag: ""
  watch:
    namespaces: []                     # empty = all
    labelSelector: ""
  resources: {}
  webhook:
    enabled: true
    certManager:
      enabled: true

mcp:
  enabled: true
  image:
    repository: ghcr.io/reactive-network/backrest-mcp
    tag: ""
  service:
    type: ClusterIP
    port: 8081
  ingress:
    enabled: false
  resources: {}

backrest:
  image:
    repository: ghcr.io/garethgeorge/backrest
    tag: ""                            # latest stable pin in Chart.yaml appVersion/annotations

monitoring:
  serviceMonitor:
    enabled: true
  vmServiceScrape:
    enabled: true
  prometheusRules:
    enabled: true
  vmRules:
    enabled: true
  grafanaDashboard:
    enabled: true
  storageUsageWarningRatio: 0.8
  storageUsageCriticalRatio: 0.95
```

**Consumer infra layout (after GitHub publish only):** a separate folder in the consumer’s infra repo with environment `values` and `apply.sh` — not shipped inside this opensource tree with private data.

---

## 11. Security model

- ClusterRoles: `backrest-viewer`, `backrest-operator`, `backrest-admin`.
- Operator SA: manage owned CRs, Jobs, VolumeSnapshots, Deployments/DaemonSets for Backrest, Secrets it creates — least privilege.
- MCP SA: authz APIs, the backrest CRs and Jobs; no impersonation.
- Repository credentials always in Secrets; document ExternalSecrets/Vault as optional.
- Append-only repos fail closed on delete/forget.

---

## 12. Documentation requirements

Docs must include:

1. **Operator usage** — install Helm, create repository/plan/PVC backup/restore.
2. **MCP usage** — HTTP and stdio setup, kube token auth, role bindings, destructive flag.
3. **Manual backup runbook** — raw `restic` + `VolumeSnapshot` without the operator.
4. **Manual restore runbook** — restore to PVC and local tarball without the operator.
5. **Operator/Backrest state backup** — how to back up host PVC/config/pairing secrets.
6. **DR** — recover control plane pieces after operator loss.

All documentation in English.

---

## 13. Build, test, release

| Item | v1 requirement |
|------|----------------|
| Docker images | `backrest-operator`, `backrest-mcp` (multi-stage, non-root) |
| Helm chart | packaged in CI; CRDs in `crds/` |
| Unit tests | handlers, webhooks validators, MCP auth/SAR mapping, strategy pipeline |
| Kind/k3d e2e | **out of scope for v1** |
| CI | lint, unit tests, build images, package chart |

---

## 14. Operator self-backup and DR

- Support (CR or documented Job) to back up Backrest host persistence and operator-managed Secrets/ConfigMaps needed to rehydrate a cluster.
- Docs must describe restoring that state onto a fresh operator install.
- Manual restic/CSI procedures remain first-class for total operator failure.

---

## 15. Phased delivery

| Phase | Scope |
|-------|--------|
| **P0** | CRDs; BackrestCluster (incl. Ingress); BackupRepository verify; PVCBackup quiesce/multi-PVC/schedule/CSI; Helm; metrics |
| **P1** | MCP Deployment, TokenReview+impersonation+SAR, RBAC roles, validating webhooks |
| **P2** | ServiceMonitor/VM*/Grafana embeds, append-only enforcement |
| **P3** | BackupPlan → Backrest sync; liveFlush; UI auth secret; full docs/DR polish; pairing probe |

---

## 16. Backrest fork policy

Prefer upstream images and APIs.

Clone/fork Backrest only when implementation hits a hard gap (examples: non-UI multihost pairing automation, missing metrics, API needed by MCP). Record gaps below; upstream PRs are encouraged.

### Gap log

| Date | Gap | Workaround | Fork needed? |
|------|-----|------------|--------------|
| 2026-07-24 | BackupPlan not applied to Backrest host | Use PVCBackup for real backups | No |
| 2026-07-24 | liveFlush / deletePods / appendOnly / UI auth secret | Omit or document as unimplemented | No |
| 2026-07-24 | Multihost pairing status | Status copies agentsReady (not a real pairing probe) | Maybe (Backrest API) |

---

## 17. Repository layout (target after implementation)

```text
backrest-operator/
  SPEC.md
  README.md
  LICENSE
  api/v1alpha1/           # Go types + CRD YAML
  cmd/operator/           # operator entrypoint
  cmd/mcp/                # MCP entrypoint
  internal/
    controller/           # reconcilers
    webhook/              # validating admission
    mcp/                  # MCP auth, tools, HTTP/stdio
    filters/
    metrics/
  charts/backrest-operator/
  docs/
  examples/
  Dockerfile
  Dockerfile.mcp
  Makefile
  go.mod
  .github/workflows/
```

---

## 18. Implementation notes

- Always unquiesce in `finally` unless explicit `leaveDown`.
- Prefer narrow Jobs with TTL after finished.
- Never log restic passwords or bearer tokens.
- Keep examples free of private domains and vendor lock-in beyond documented optional drivers (TopoLVM, CSI).
- Chart `home` / `sources` point at this GitHub repository (personal account) and upstream Backrest.
- **UI Ingress belongs in `BackrestCluster.spec.host.ingress`.** oauth2-proxy may be a separate Deployment; point `backendServiceName` at it. Do not teach hand-rolled Ingress as the primary path.
- **Do not invent new node label keys** for agents; use existing labels and matching tolerations.
