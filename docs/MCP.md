# Backrest MCP Server

The **backrest-mcp** Deployment exposes Model Context Protocol (MCP) tools so AI agents and automation can manage backups under Kubernetes RBAC — not under the MCP pod's own ServiceAccount.

## Deployment

The MCP server ships with the Helm chart (`mcp.enabled: true` by default). It runs as a separate Deployment from the operator for process isolation.

```bash
kubectl get svc -n backrest-system -l app.kubernetes.io/component=mcp
```

Default Service port: `8081` (HTTP JSON-RPC at `/mcp`).

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `MCP_MODE` | `http` | `http` or `stdio` |
| `MCP_PORT` | `8081` | HTTP listen port |
| `METRICS_PORT` | `8080` | Prometheus metrics |

## HTTP mode — Bearer Kubernetes token

Remote clients authenticate with a user or ServiceAccount token:

```bash
TOKEN=$(kubectl create token backrest-user -n app --duration=1h)

curl -sS -X POST "http://127.0.0.1:8081/mcp" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
```

### Authentication flow

1. Client sends `Authorization: Bearer <token>`.
2. MCP validates the token via Kubernetes **TokenReview**.
3. Each tool call maps to a **SubjectAccessReview** against CR resources in `operator.backrest.io`.
4. A tool runs only when its SubjectAccessReview allows the caller; the call itself then uses the MCP pod ServiceAccount.

The MCP pod ServiceAccount holds `tokenreviews`, `subjectaccessreviews`, the `operator.backrest.io` CRs and Jobs. It has no `impersonate`: an `impersonate` rule without `resourceNames` lets the SA act as `system:masters`, which is cluster-admin.

When the Backrest host has auth enabled, set the chart value `backrestAuth.existingSecret` so the MCP container gets `BACKREST_AUTH_USERNAME` / `BACKREST_AUTH_PASSWORD`.

Port-forward for local access:

```bash
kubectl port-forward -n backrest-system svc/backrest-operator-mcp 8081:8081
```

## stdio mode — local IDE / CLI

Run MCP with stdio transport for Cursor or other local MCP clients:

```bash
MCP_MODE=stdio /mcp
# or locally:
MCP_MODE=stdio ./bin/mcp
```

In stdio mode, the server uses the caller's kubeconfig identity (`mcp-stdio-local`). Destructive tools still require `allow_destructive=true`.

Example Cursor MCP config:

```json
{
  "mcpServers": {
    "backrest": {
      "command": "/path/to/mcp",
      "env": {
        "MCP_MODE": "stdio"
      }
    }
  }
}
```

## RBAC roles

Bind users to one of the shipped ClusterRoles before calling MCP tools:

| ClusterRole | Typical use |
|-------------|-------------|
| `backrest-viewer` | Read clusters, repositories, plans, backup/restore status |
| `backrest-operator` | Create plans, trigger backups, create restores and exports |
| `backrest-admin` | Delete repositories/plans, manage secrets, destructive overrides |

Example binding — see [examples/rbac-binding.yaml](../examples/rbac-binding.yaml).

## Destructive operations — `allow_destructive`

Tools that delete data require an explicit opt-in flag. Default is **false**.

Destructive tools:

- `delete_repository`
- `delete_plan`
- `delete_snapshot`

Example tool call:

```json
{
  "jsonrpc": "2.0",
  "id": 2,
  "method": "tools/call",
  "params": {
    "name": "delete_plan",
    "arguments": {
      "namespace": "app",
      "name": "old-plan",
      "allow_destructive": true
    }
  }
}
```

Without `allow_destructive: true`, the server returns HTTP 403 / `PermissionError`.

Append-only repositories additionally block forget/delete operations unless the caller has admin RBAC.

## Tool catalog summary

| Tool | Action |
|------|--------|
| `list_clusters` | List BackrestCluster resources |
| `get_cluster` | Get a BackrestCluster |
| `list_repositories` | List BackupRepositories in a namespace |
| `get_repository` | Get a BackupRepository |
| `create_repository` | Create a BackupRepository |
| `delete_repository` | Delete a repository (**destructive**) |
| `list_plans` | List BackupPlans |
| `create_plan` | Create a BackupPlan |
| `delete_plan` | Delete a BackupPlan (**destructive**) |
| `trigger_backup` | Create a PVCBackup (on-demand backup) |
| `create_pvc_restore` | Create a PVCRestore |
| `get_pvc_restore` | Get restore status |
| `get_snapshot_download_url` | Signed Backrest URL (immediate) |
| `create_snapshot_download` | Create SnapshotDownload CR; waits for `status.downloadURL` |
| `get_snapshot_download` | Read SnapshotDownload status / URL |
| `repo_status` | Repository phase, verify status, append-only flag |

Tools declared in the spec but not yet exposed may appear in future releases — check `ToolSchemas()` in the source for the current list.

## Metrics

MCP exposes `backrest_mcp_auth_denials_total{tool="..."}` when authorization fails. Scrape the metrics port alongside operator metrics.

## Related docs

- [Operator usage](./USAGE.md)
- [RBAC binding example](../examples/rbac-binding.yaml)
