package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"

	"github.com/Reactive-Network/backrest-operator/internal/backrest"
)

var (
	gvrCluster = schema.GroupVersionResource{Group: "operator.backrest.io", Version: "v1alpha1", Resource: "backrestclusters"}
	gvrRepo    = schema.GroupVersionResource{Group: "operator.backrest.io", Version: "v1alpha1", Resource: "backuprepositories"}
	gvrPlan    = schema.GroupVersionResource{Group: "operator.backrest.io", Version: "v1alpha1", Resource: "backupplans"}
	gvrBackup           = schema.GroupVersionResource{Group: "operator.backrest.io", Version: "v1alpha1", Resource: "pvcbackups"}
	gvrRestore          = schema.GroupVersionResource{Group: "operator.backrest.io", Version: "v1alpha1", Resource: "pvcrestores"}
	gvrSnapshotDownload = schema.GroupVersionResource{Group: "operator.backrest.io", Version: "v1alpha1", Resource: "snapshotdownloads"}
)

type Tools struct {
	base *rest.Config
}

func NewTools(cfg *rest.Config) *Tools {
	return &Tools{base: cfg}
}

// clientFor always uses the pod's own ServiceAccount. The caller was already checked with a
// SubjectAccessReview in AuthorizeTool; impersonating the caller needed an unrestricted
// impersonate rule, which is cluster-admin (groups/system:masters).
func (t *Tools) clientFor(_ *UserIdentity) (dynamic.Interface, error) {
	return dynamic.NewForConfig(t.base)
}

func (t *Tools) Call(ctx context.Context, user *UserIdentity, name string, args map[string]interface{}) (interface{}, error) {
	ns := strArg(args, "namespace", "default")
	allow := boolArg(args, "allow_destructive")
	dc, err := t.clientFor(user)
	if err != nil {
		return nil, err
	}

	switch name {
	case "list_clusters":
		listNS := strArg(args, "namespace", "")
		if listNS == "" {
			list, err := dc.Resource(gvrCluster).List(ctx, metav1.ListOptions{})
			if err != nil {
				return nil, err
			}
			return list.Items, nil
		}
		list, err := dc.Resource(gvrCluster).Namespace(listNS).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, err
		}
		return list.Items, nil
	case "get_cluster":
		return dc.Resource(gvrCluster).Namespace(ns).Get(ctx, strArg(args, "name", ""), metav1.GetOptions{})
	case "list_repositories":
		list, err := dc.Resource(gvrRepo).Namespace(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, err
		}
		return list.Items, nil
	case "get_repository":
		return dc.Resource(gvrRepo).Namespace(ns).Get(ctx, strArg(args, "name", ""), metav1.GetOptions{})
	case "create_repository":
		obj, err := bodyToUnstructured(args["body"], "BackupRepository", ns)
		if err != nil {
			return nil, err
		}
		return dc.Resource(gvrRepo).Namespace(ns).Create(ctx, obj, metav1.CreateOptions{})
	case "delete_repository":
		if !allow {
			return nil, fmt.Errorf("allow_destructive=true required")
		}
		err := dc.Resource(gvrRepo).Namespace(ns).Delete(ctx, strArg(args, "name", ""), metav1.DeleteOptions{})
		if err != nil {
			return nil, err
		}
		return map[string]string{"deleted": strArg(args, "name", "")}, nil
	case "list_plans":
		list, err := dc.Resource(gvrPlan).Namespace(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, err
		}
		return list.Items, nil
	case "create_plan":
		obj, err := bodyToUnstructured(args["body"], "BackupPlan", ns)
		if err != nil {
			return nil, err
		}
		return dc.Resource(gvrPlan).Namespace(ns).Create(ctx, obj, metav1.CreateOptions{})
	case "delete_plan":
		if !allow {
			return nil, fmt.Errorf("allow_destructive=true required")
		}
		err := dc.Resource(gvrPlan).Namespace(ns).Delete(ctx, strArg(args, "name", ""), metav1.DeleteOptions{})
		if err != nil {
			return nil, err
		}
		return map[string]string{"deleted": strArg(args, "name", "")}, nil
	case "trigger_backup":
		name := strArg(args, "name", "")
		if name != "" {
			obj, err := dc.Resource(gvrBackup).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return nil, err
			}
			anns := obj.GetAnnotations()
			if anns == nil {
				anns = map[string]string{}
			}
			token := strArg(args, "token", "")
			if token == "" {
				token = fmt.Sprintf("%d", time.Now().UTC().Unix())
			}
			anns["operator.backrest.io/force-run"] = token
			obj.SetAnnotations(anns)
			updated, err := dc.Resource(gvrBackup).Namespace(ns).Update(ctx, obj, metav1.UpdateOptions{})
			if err != nil {
				return nil, err
			}
			return map[string]interface{}{
				"forced":  true,
				"name":    name,
				"token":   token,
				"resource": updated,
			}, nil
		}
		obj, err := bodyToUnstructured(args["body"], "PVCBackup", ns)
		if err != nil {
			return nil, err
		}
		return dc.Resource(gvrBackup).Namespace(ns).Create(ctx, obj, metav1.CreateOptions{})
	case "create_pvc_restore":
		obj, err := bodyToUnstructured(args["body"], "PVCRestore", ns)
		if err != nil {
			return nil, err
		}
		return dc.Resource(gvrRestore).Namespace(ns).Create(ctx, obj, metav1.CreateOptions{})
	case "get_pvc_restore":
		return dc.Resource(gvrRestore).Namespace(ns).Get(ctx, strArg(args, "name", ""), metav1.GetOptions{})
	case "repo_status":
		obj, err := dc.Resource(gvrRepo).Namespace(ns).Get(ctx, strArg(args, "name", ""), metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		spec, _, _ := unstructured.NestedMap(obj.Object, "spec")
		status, _, _ := unstructured.NestedMap(obj.Object, "status")
		meta, _, _ := unstructured.NestedMap(obj.Object, "metadata")
		return map[string]interface{}{
			"metadata": meta,
			"status":   status,
			"spec": map[string]interface{}{
				"url":        spec["url"],
				"appendOnly": spec["appendOnly"],
			},
		}, nil
	case "list_snapshots":
		return t.listSnapshots(ctx, dc, ns, args)
	case "get_snapshot_download_url":
		return t.getSnapshotDownloadURL(ctx, dc, ns, args)
	case "create_snapshot_download":
		return t.createSnapshotDownload(ctx, dc, ns, args)
	case "get_snapshot_download":
		return t.getSnapshotDownload(ctx, dc, ns, args)
	case "get_host_config":
		return t.getHostConfig(ctx, args)
	case "index_repository":
		return t.indexRepository(ctx, dc, ns, args)
	default:
		return nil, fmt.Errorf("unknown tool %s", name)
	}
}

func (t *Tools) hostClientFromArgs(ctx context.Context, dc dynamic.Interface, ns string, args map[string]interface{}, repoName string) (*backrest.Client, string, error) {
	clusterNS := strArg(args, "cluster_namespace", "")
	clusterName := strArg(args, "cluster_name", "")
	if clusterName == "" && repoName != "" {
		obj, err := dc.Resource(gvrRepo).Namespace(ns).Get(ctx, repoName, metav1.GetOptions{})
		if err != nil {
			return nil, "", err
		}
		if v, ok, _ := unstructured.NestedString(obj.Object, "spec", "backrest", "clusterRef", "namespace"); ok && v != "" {
			clusterNS = v
		}
		if v, ok, _ := unstructured.NestedString(obj.Object, "spec", "backrest", "clusterRef", "name"); ok && v != "" {
			clusterName = v
		}
	}
	if clusterNS == "" {
		clusterNS = "backrest"
	}
	if clusterName == "" {
		clusterName = "main"
	}
	return backrest.NewClient(backrest.HostURL(clusterNS, clusterName)), clusterName, nil
}

func (t *Tools) listSnapshots(ctx context.Context, dc dynamic.Interface, ns string, args map[string]interface{}) (interface{}, error) {
	name := strArg(args, "name", "")
	planID := strArg(args, "plan_id", "")
	bc, _, err := t.hostClientFromArgs(ctx, dc, ns, args, name)
	if err != nil {
		return nil, err
	}
	snaps, err := bc.ListSnapshots(ctx, name, planID)
	if err != nil {
		return nil, err
	}
	return map[string]interface{}{"repository": name, "planId": planID, "snapshots": snaps, "count": len(snaps)}, nil
}

func (t *Tools) getSnapshotDownloadURL(ctx context.Context, dc dynamic.Interface, ns string, args map[string]interface{}) (interface{}, error) {
	repo := strArg(args, "name", "")
	snapID := strArg(args, "snapshot_id", "")
	if repo == "" || snapID == "" {
		return nil, fmt.Errorf("name and snapshot_id are required")
	}
	path := strArg(args, "path", "/")
	planID := strArg(args, "plan_id", "")
	mode := strArg(args, "mode", "restore")
	publicBase := strArg(args, "public_base_url", "")
	if publicBase == "" {
		publicBase = strings.TrimSpace(os.Getenv("BACKREST_PUBLIC_BASE_URL"))
	}
	if publicBase == "" {
		publicBase = "https://backup.prq-infra.net"
	}
	bc, _, err := t.hostClientFromArgs(ctx, dc, ns, args, repo)
	if err != nil {
		return nil, err
	}
	target := ""
	if mode == "" || mode == "restore" {
		target = fmt.Sprintf("/data/snapdl/mcp/%s/%d", min8(snapID), time.Now().Unix())
	}
	link, err := bc.MintDownloadURL(ctx, repo, snapID, planID, path, publicBase, mode, target)
	if err != nil {
		return nil, err
	}
	out := map[string]interface{}{
		"downloadURL": link.DownloadURL,
		"relativeURL": link.RelativeURL,
		"operationId": link.OperationID,
		"snapshotId":  snapID,
		"path":        link.Path,
		"repository":  repo,
		"mode":        link.Mode,
		"contentType": "application/octet-stream (.tar)",
		"note":        "mode=restore schedules Backrest Restore (visible in UI). mode=stream skips Restore.",
	}
	if !link.ExpiresAt.IsZero() {
		out["expiresAt"] = link.ExpiresAt.UTC().Format(time.RFC3339)
	}
	return out, nil
}

func min8(s string) string {
	if len(s) < 8 {
		return s
	}
	return s[:8]
}

func (t *Tools) createSnapshotDownload(ctx context.Context, dc dynamic.Interface, ns string, args map[string]interface{}) (interface{}, error) {
	repo := strArg(args, "repository_name", strArg(args, "name", ""))
	snapID := strArg(args, "snapshot_id", "")
	if repo == "" || snapID == "" {
		return nil, fmt.Errorf("repository_name and snapshot_id are required")
	}
	repoNS := strArg(args, "repository_namespace", ns)
	path := strArg(args, "path", "/")
	planID := strArg(args, "plan_id", "")
	mode := strArg(args, "mode", "restore")
	publicBase := strArg(args, "public_base_url", "")
	waitSec := intArg(args, "wait_seconds", 300)
	if waitSec < 5 {
		waitSec = 5
	}
	name := strArg(args, "download_name", "")
	meta := map[string]interface{}{"namespace": ns}
	if name != "" {
		meta["name"] = name
	} else {
		meta["generateName"] = "snapdl-"
	}
	spec := map[string]interface{}{
		"repositoryRef": map[string]interface{}{"name": repo, "namespace": repoNS},
		"snapshotID":    snapID,
		"path":          path,
		"mode":          mode,
	}
	if planID != "" {
		spec["planID"] = planID
	}
	if publicBase != "" {
		spec["publicBaseURL"] = publicBase
	}
	obj := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "operator.backrest.io/v1alpha1",
		"kind":       "SnapshotDownload",
		"metadata":   meta,
		"spec":       spec,
	}}
	created, err := dc.Resource(gvrSnapshotDownload).Namespace(ns).Create(ctx, obj, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}
	crName := created.GetName()
	deadline := time.Now().Add(time.Duration(waitSec) * time.Second)
	var latest *unstructured.Unstructured
	for {
		latest, err = dc.Resource(gvrSnapshotDownload).Namespace(ns).Get(ctx, crName, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		phase, _, _ := unstructured.NestedString(latest.Object, "status", "phase")
		url, _, _ := unstructured.NestedString(latest.Object, "status", "downloadURL")
		if phase == "Ready" && url != "" {
			break
		}
		if phase == "Failed" {
			msg, _, _ := unstructured.NestedString(latest.Object, "status", "message")
			return map[string]interface{}{
				"name": crName, "namespace": ns, "phase": phase, "message": msg,
			}, fmt.Errorf("SnapshotDownload %s/%s failed: %s", ns, crName, msg)
		}
		if time.Now().After(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	phase, _, _ := unstructured.NestedString(latest.Object, "status", "phase")
	url, _, _ := unstructured.NestedString(latest.Object, "status", "downloadURL")
	expires, _, _ := unstructured.NestedString(latest.Object, "status", "expiresAt")
	msg, _, _ := unstructured.NestedString(latest.Object, "status", "message")
	opID, _, _ := unstructured.NestedFieldNoCopy(latest.Object, "status", "operationID")
	out := map[string]interface{}{
		"name": crName, "namespace": ns, "phase": phase,
		"downloadURL": url, "expiresAt": expires, "message": msg,
		"operationID": opID, "mode": mode,
		"snapshotId": snapID, "repository": repo,
		"note": "Restore appears in Backrest UI when mode=restore. URL also in .status.downloadURL.",
	}
	if url == "" {
		return out, fmt.Errorf("SnapshotDownload %s/%s created but downloadURL not ready yet (phase=%s); retry get_snapshot_download", ns, crName, phase)
	}
	return out, nil
}

func (t *Tools) getSnapshotDownload(ctx context.Context, dc dynamic.Interface, ns string, args map[string]interface{}) (interface{}, error) {
	name := strArg(args, "name", "")
	if name == "" {
		return nil, fmt.Errorf("name is required")
	}
	obj, err := dc.Resource(gvrSnapshotDownload).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	status, _, _ := unstructured.NestedMap(obj.Object, "status")
	return map[string]interface{}{
		"name":        obj.GetName(),
		"namespace":   obj.GetNamespace(),
		"phase":       status["phase"],
		"downloadURL": status["downloadURL"],
		"expiresAt":   status["expiresAt"],
		"relativeURL": status["relativeURL"],
		"operationID": status["operationID"],
		"message":     status["message"],
		"snapshotID":  status["snapshotID"],
		"path":        status["path"],
	}, nil
}

func (t *Tools) getHostConfig(ctx context.Context, args map[string]interface{}) (interface{}, error) {
	clusterNS := strArg(args, "cluster_namespace", "backrest")
	clusterName := strArg(args, "cluster_name", "main")
	bc := backrest.NewClient(backrest.HostURL(clusterNS, clusterName))
	cfg, err := bc.GetConfig(ctx)
	if err != nil {
		return nil, err
	}
	// Redact secrets before returning to MCP clients.
	for i := range cfg.Repos {
		if cfg.Repos[i].Password != "" {
			cfg.Repos[i].Password = "***"
		}
		if len(cfg.Repos[i].Env) > 0 {
			redacted := make([]string, len(cfg.Repos[i].Env))
			for j := range cfg.Repos[i].Env {
				redacted[j] = "***"
			}
			cfg.Repos[i].Env = redacted
		}
	}
	return cfg, nil
}

func (t *Tools) indexRepository(ctx context.Context, dc dynamic.Interface, ns string, args map[string]interface{}) (interface{}, error) {
	name := strArg(args, "name", "")
	bc, _, err := t.hostClientFromArgs(ctx, dc, ns, args, name)
	if err != nil {
		return nil, err
	}
	if err := bc.IndexSnapshots(ctx, name); err != nil {
		return nil, err
	}
	return map[string]string{"indexed": name}, nil
}

func bodyToUnstructured(body interface{}, kind, ns string) (*unstructured.Unstructured, error) {
	if body == nil {
		return nil, fmt.Errorf("body is required")
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	obj := &unstructured.Unstructured{}
	if err := obj.UnmarshalJSON(raw); err != nil {
		return nil, err
	}
	obj.SetAPIVersion("operator.backrest.io/v1alpha1")
	obj.SetKind(kind)
	obj.SetNamespace(ns)
	return obj, nil
}

func strArg(args map[string]interface{}, key, def string) string {
	if args == nil {
		return def
	}
	v, ok := args[key]
	if !ok || v == nil {
		return def
	}
	s, ok := v.(string)
	if !ok {
		return def
	}
	if s == "" {
		return def
	}
	return s
}

func boolArg(args map[string]interface{}, key string) bool {
	if args == nil {
		return false
	}
	v, ok := args[key]
	if !ok {
		return false
	}
	b, ok := v.(bool)
	return ok && b
}

func intArg(args map[string]interface{}, key string, def int) int {
	if args == nil {
		return def
	}
	v, ok := args[key]
	if !ok || v == nil {
		return def
	}
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case int64:
		return int(n)
	default:
		return def
	}
}

func stringSliceArg(args map[string]interface{}, key string) []string {
	if args == nil {
		return []string{}
	}
	v, ok := args[key]
	if !ok || v == nil {
		return []string{}
	}
	arr, ok := v.([]interface{})
	if !ok {
		return []string{}
	}
	out := make([]string, 0, len(arr))
	for _, item := range arr {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// IsNotFound reports whether err is a Kubernetes not-found error.
func IsNotFound(err error) bool {
	return apierrors.IsNotFound(err)
}
