package controller

import (
	"context"
	"fmt"
	"os"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	operatorv1alpha1 "github.com/Reactive-Network/backrest-operator/api/v1alpha1"
	"github.com/Reactive-Network/backrest-operator/internal/backrest"
	"github.com/Reactive-Network/backrest-operator/internal/filters"
	"github.com/Reactive-Network/backrest-operator/internal/logging"
	"github.com/Reactive-Network/backrest-operator/internal/metrics"
)

const annRefreshDownload = "operator.backrest.io/refresh"

type SnapshotDownloadReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

func (r *SnapshotDownloadReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("snapshotdownload", req.NamespacedName)
	var sd operatorv1alpha1.SnapshotDownload
	if err := r.Get(ctx, req.NamespacedName, &sd); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !filters.ObjectAllowed(sd.Namespace, sd.Labels) {
		logger.V(1).Info("skipped by watch filter")
		return ctrl.Result{}, nil
	}

	refresh := ""
	if sd.Annotations != nil {
		refresh = sd.Annotations[annRefreshDownload]
	}
	if sd.Status.Phase == "Ready" && sd.Status.DownloadURL != "" && refresh == sd.Status.LastRefresh {
		if sd.Status.ExpiresAt != "" {
			if t, err := time.Parse(time.RFC3339, sd.Status.ExpiresAt); err == nil && time.Now().Before(t.Add(-2*time.Minute)) {
				return ctrl.Result{RequeueAfter: time.Until(t.Add(-2 * time.Minute))}, nil
			}
		} else {
			return ctrl.Result{}, nil
		}
	}

	sd.Status.Phase = "Pending"
	sd.Status.Message = "minting download URL"
	_ = r.Status().Update(ctx, &sd)
	logger.Info("minting download URL", "mode", sd.Spec.Mode, "snapshot", logging.Truncate(sd.Spec.SnapshotID, 12))

	link, err := r.mint(ctx, &sd)
	if err != nil {
		logger.Error(err, "failed to mint download URL")
		metrics.ReconcileErrors.WithLabelValues("SnapshotDownload").Inc()
		sd.Status.Phase = "Failed"
		sd.Status.Message = err.Error()
		sd.Status.Conditions = []operatorv1alpha1.Condition{{Type: "Ready", Status: "False", Message: err.Error()}}
		_ = r.Status().Update(ctx, &sd)
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	sd.Status.Phase = "Ready"
	sd.Status.DownloadURL = link.DownloadURL
	sd.Status.RelativeURL = link.RelativeURL
	sd.Status.OperationID = link.OperationID
	sd.Status.SnapshotID = sd.Spec.SnapshotID
	sd.Status.Path = link.Path
	if link.Mode == "restore" {
		sd.Status.Message = "Backrest Restore completed; download URL ready (visible in UI)"
	} else {
		sd.Status.Message = "stream URL ready (index snapshot; no Restore op in UI)"
	}
	sd.Status.LastRefresh = refresh
	sd.Status.Conditions = []operatorv1alpha1.Condition{{Type: "Ready", Status: "True", Message: "URL ready"}}
	if !link.ExpiresAt.IsZero() {
		sd.Status.ExpiresAt = link.ExpiresAt.UTC().Format(time.RFC3339)
	} else {
		sd.Status.ExpiresAt = ""
	}
	if err := r.Status().Update(ctx, &sd); err != nil {
		return ctrl.Result{}, err
	}
	logger.Info("download URL ready",
		"mode", link.Mode,
		"operationID", link.OperationID,
		"expiresAt", sd.Status.ExpiresAt,
		"url", logging.RedactURL(link.DownloadURL),
	)
	logger.V(1).Info("download URL (full)", "url", link.DownloadURL)
	if !link.ExpiresAt.IsZero() {
		until := time.Until(link.ExpiresAt.Add(-2 * time.Minute))
		if until < time.Minute {
			until = time.Minute
		}
		return ctrl.Result{RequeueAfter: until}, nil
	}
	return ctrl.Result{}, nil
}

func (r *SnapshotDownloadReconciler) mint(ctx context.Context, sd *operatorv1alpha1.SnapshotDownload) (*backrest.DownloadLink, error) {
	repoNS := sd.Spec.RepositoryRef.Namespace
	if repoNS == "" {
		repoNS = sd.Namespace
	}
	var repo operatorv1alpha1.BackupRepository
	if err := r.Get(ctx, types.NamespacedName{Name: sd.Spec.RepositoryRef.Name, Namespace: repoNS}, &repo); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("BackupRepository %s/%s not found", repoNS, sd.Spec.RepositoryRef.Name)
		}
		return nil, err
	}
	clusterNS, clusterName := resolveClusterRef(repo.Spec.Backrest.ClusterRef, repo.Namespace)
	bc, _, err := hostClientForCluster(ctx, r.Client, clusterNS, clusterName)
	if err != nil {
		return nil, err
	}

	publicBase := sd.Spec.PublicBaseURL
	if publicBase == "" {
		publicBase = os.Getenv("BACKREST_PUBLIC_BASE_URL")
	}
	if publicBase == "" {
		publicBase = "https://backup.prq-infra.net"
	}
	path := sd.Spec.Path
	if path == "" {
		path = "/"
	}
	mode := sd.Spec.Mode
	if mode == "" {
		mode = "restore"
	}
	target := ""
	if mode == "restore" {
		target = fmt.Sprintf("/data/snapdl/%s/%s/%d", sd.Namespace, sd.Name, time.Now().Unix())
	}
	link, err := bc.MintDownloadURL(ctx, repo.Name, sd.Spec.SnapshotID, sd.Spec.PlanID, path, publicBase, mode, target)
	if err != nil {
		return nil, err
	}
	return link, nil
}

func (r *SnapshotDownloadReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&operatorv1alpha1.SnapshotDownload{}).
		Complete(r)
}
