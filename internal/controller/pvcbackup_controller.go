package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	operatorv1alpha1 "github.com/Reactive-Network/backrest-operator/api/v1alpha1"
	"github.com/Reactive-Network/backrest-operator/internal/backrest"
	"github.com/Reactive-Network/backrest-operator/internal/filters"
	"github.com/Reactive-Network/backrest-operator/internal/metrics"
)

const (
	annForceRun       = "operator.backrest.io/force-run"
	annForcePrune     = "operator.backrest.io/force-prune"
	annQuiesceState   = "operator.backrest.io/quiesce-state"
	finalizerHostPlan = "operator.backrest.io/host-plan"

	podDeleteTimeout = 5 * time.Minute
)

var volumeSnapshotGVK = schema.GroupVersionKind{
	Group: "snapshot.storage.k8s.io", Version: "v1", Kind: "VolumeSnapshot",
}

type PVCBackupReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

func (r *PVCBackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("pvcbackup", req.NamespacedName)
	var backup operatorv1alpha1.PVCBackup
	if err := r.Get(ctx, req.NamespacedName, &backup); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !filters.ObjectAllowed(backup.Namespace, backup.Labels) {
		logger.V(1).Info("skipped by watch filter")
		return ctrl.Result{}, nil
	}
	metrics.SyncBackupLastSuccess(backup.Namespace, backup.Name, metrics.LastSuccessUnixFromStatus(backup.Status))

	if !backup.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&backup, finalizerHostPlan) {
			if err := removePVCBackupPlanFromHost(ctx, r.Client, &backup); err != nil {
				logger.Error(err, "remove host plan on delete")
				return ctrl.Result{Requeue: true}, err
			}
			controllerutil.RemoveFinalizer(&backup, finalizerHostPlan)
			if err := r.Update(ctx, &backup); err != nil {
				return ctrl.Result{Requeue: true}, err
			}
		}
		return ctrl.Result{}, nil
	}
	if !controllerutil.ContainsFinalizer(&backup, finalizerHostPlan) {
		controllerutil.AddFinalizer(&backup, finalizerHostPlan)
		if err := r.Update(ctx, &backup); err != nil {
			return ctrl.Result{Requeue: true}, err
		}
	}

	pvcs := pvcList(&backup)
	if len(pvcs) == 0 {
		return r.fail(ctx, &backup, fmt.Errorf("pvcName or pvcNames required"))
	}

	// Drop kubectl-annotate CronJob scaffolding from older operator versions.
	_ = r.cleanupLegacyScheduleResources(ctx, &backup)

	// Resume watching an in-flight backup Job without re-quiescing.
	// One Job per PVC session; a restore to the same disk may interrupt us via fail().
	if backup.Status.Phase == "Uploading" && backup.Status.LastJobName != "" {
		return r.pollBackupJob(ctx, &backup)
	}

	// Resume watching an in-flight prune-only Job (no quiesce, no PVC mount).
	if backup.Status.Phase == "Pruning" && backup.Status.LastJobName != "" {
		return r.pollPruneJob(ctx, &backup)
	}

	// Start force-prune before backup/schedule paths.
	if forcePrunePending(&backup) {
		if backupPhaseInFlight(backup.Status.Phase) {
			logger.V(1).Info("backup in-flight; deferring force-prune", "phase", backup.Status.Phase)
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		if backup.Spec.Retention.KeepLast == nil || *backup.Spec.Retention.KeepLast <= 0 {
			if force := backup.Annotations[annForcePrune]; force != "" {
				backup.Status.LastForcePrune = force
				_ = r.Status().Update(ctx, &backup)
			}
			return r.failPrune(ctx, &backup, fmt.Errorf("spec.retention.keepLast required for prune (must be > 0)"))
		}
		if force := backup.Annotations[annForcePrune]; force != "" {
			backup.Status.LastForcePrune = force
		}
		backup.Status.Phase = "Pruning"
		if err := r.Status().Update(ctx, &backup); err != nil {
			return ctrl.Result{}, err
		}
		repoNS := backup.Spec.RepositoryRef.Namespace
		if repoNS == "" {
			repoNS = backup.Namespace
		}
		var repo operatorv1alpha1.BackupRepository
		if err := r.Get(ctx, types.NamespacedName{Name: backup.Spec.RepositoryRef.Name, Namespace: repoNS}, &repo); err != nil {
			return r.failPrune(ctx, &backup, err)
		}
		if err := r.ensureRepoSecrets(ctx, &backup, &repo); err != nil {
			return r.failPrune(ctx, &backup, err)
		}
		started := time.Now()
		jobName := fmt.Sprintf("pvcbackup-prune-%s-%d", backup.Name, started.Unix())
		if len(jobName) > 63 {
			jobName = jobName[:63]
		}
		logger.Info("creating restic prune job", "job", jobName, "keepLast", *backup.Spec.Retention.KeepLast)
		if err := r.createResticPruneJob(ctx, &backup, &repo, jobName); err != nil {
			return r.failPrune(ctx, &backup, err)
		}
		backup.Status.LastJobName = jobName
		if err := r.Status().Update(ctx, &backup); err != nil {
			logger.Error(err, "status update after prune job create; will requeue to poll existing job")
			return ctrl.Result{Requeue: true}, nil
		}
		return r.pollPruneJob(ctx, &backup)
	}

	// Recover from status conflicts: an owned prune Job may exist without Phase=Pruning.
	if jobName, ok := r.findOwnedPruneJob(ctx, &backup); ok {
		logger.Info("adopting existing prune job", "job", jobName)
		backup.Status.Phase = "Pruning"
		backup.Status.LastJobName = jobName
		if err := r.Status().Update(ctx, &backup); err != nil {
			return ctrl.Result{Requeue: true}, nil
		}
		return r.pollPruneJob(ctx, &backup)
	}

	if backup.Status.Phase == "Failed" && !forceRunPending(&backup) && !forcePrunePending(&backup) && backup.Spec.Schedule == "" {
		return ctrl.Result{}, nil
	}
	// Recover from status conflicts: an owned Job may already exist without Phase=Uploading.
	if jobName, ok := r.findOwnedBackupJob(ctx, &backup); ok {
		logger.Info("adopting existing restic job", "job", jobName)
		backup.Status.Phase = "Uploading"
		backup.Status.LastJobName = jobName
		if err := r.Status().Update(ctx, &backup); err != nil {
			return ctrl.Result{Requeue: true}, nil
		}
		return r.pollBackupJob(ctx, &backup)
	}

	// Do not start backup while prune is pending or in-flight.
	if backup.Status.Phase == "Pruning" || forcePrunePending(&backup) {
		if backup.Status.Phase == "Pruning" && backup.Status.LastJobName != "" {
			return r.pollPruneJob(ctx, &backup)
		}
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	if backup.Spec.Schedule != "" {
		due, wait, err := scheduleDue(&backup)
		if err != nil {
			return r.fail(ctx, &backup, err)
		}
		if !due {
			if backup.Status.Phase != "Scheduled" && backup.Status.Phase != "Failed" {
				backup.Status.Phase = "Scheduled"
				_ = r.Status().Update(ctx, &backup)
			}
			logger.V(1).Info("waiting for next schedule", "schedule", backup.Spec.Schedule, "requeueAfter", wait.String())
			repoNS := backup.Spec.RepositoryRef.Namespace
			if repoNS == "" {
				repoNS = backup.Namespace
			}
			var repo operatorv1alpha1.BackupRepository
			if err := r.Get(ctx, types.NamespacedName{Name: backup.Spec.RepositoryRef.Name, Namespace: repoNS}, &repo); err == nil {
				if err := syncPVCBackupPlanToHost(ctx, r.Client, &backup, &repo); err != nil {
					logger.V(1).Info("plan sync while waiting failed", "error", err.Error())
				}
			}
			if wait <= 0 {
				wait = time.Minute
			}
			return ctrl.Result{RequeueAfter: wait}, nil
		}
		logger.Info("starting scheduled backup", "schedule", backup.Spec.Schedule)
	} else if !forceRunPending(&backup) {
		// One-shot backups idle after success/failure until a new force-run token.
		switch backup.Status.Phase {
		case "Succeeded", "Failed", "Scheduled":
			return ctrl.Result{}, nil
		}
	}

	// Claim force-run before quiesce/job create so requeues cannot start parallel runs.
	if force := backup.Annotations[annForceRun]; force != "" && force != backup.Status.LastForceRun {
		backup.Status.LastForceRun = force
		if err := r.Status().Update(ctx, &backup); err != nil {
			return ctrl.Result{}, err
		}
	}

	started := time.Now()
	var quiesceState map[string]int32
	holdQuiesceForJob := false
	defer func() {
		// Keep workloads down while the restic Job is still running.
		if holdQuiesceForJob {
			return
		}
		if !backup.Spec.Quiesce.LeaveDown && len(quiesceState) > 0 {
			logger.Info("restoring workloads after failed/aborted run")
			_ = r.unquiesce(ctx, backup.Namespace, quiesceState)
		}
	}()

	pipeline := backup.Spec.Strategy.Pipeline
	if len(pipeline) == 0 {
		pipeline = []string{"quiescedLive"}
	}
	logger.Info("backup run started", "pvcs", pvcs, "pipeline", pipeline, "repository", backup.Spec.RepositoryRef.Name)

	repoNS := backup.Spec.RepositoryRef.Namespace
	if repoNS == "" {
		repoNS = backup.Namespace
	}
	var repo operatorv1alpha1.BackupRepository
	if err := r.Get(ctx, types.NamespacedName{Name: backup.Spec.RepositoryRef.Name, Namespace: repoNS}, &repo); err != nil {
		return r.fail(ctx, &backup, err)
	}
	// Serialize mounts: wait if another backup/restore holds an overlapping PVC.
	// (SnapshotDownload and restores to a different disk do not block us.)
	if busy, holder, err := pvcSessionBusy(ctx, r.Client, backupPVCKeys(&backup), "PVCBackup", backup.Namespace, backup.Name); err != nil {
		return ctrl.Result{}, err
	} else if busy {
		logger.Info("PVC session busy; waiting", "holder", holder, "pvcs", pvcs)
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if err := r.ensureRepoSecrets(ctx, &backup, &repo); err != nil {
		return r.fail(ctx, &backup, err)
	}

	needQuiesce := backup.Spec.Quiesce.Enabled
	for _, s := range pipeline {
		if s == "quiescedLive" {
			needQuiesce = true
		}
	}
	if needQuiesce {
		backup.Status.Phase = "Quiescing"
		if err := r.Status().Update(ctx, &backup); err != nil {
			return ctrl.Result{Requeue: true}, err
		}
		logger.Info("quiescing workloads", "targets", len(backup.Spec.Quiesce.Targets))
		var err error
		quiesceState, err = r.quiesce(ctx, &backup)
		if err != nil {
			return r.fail(ctx, &backup, err)
		}
		logger.Info("workloads quiesced", "scaled", len(quiesceState))
	}

	uploadPVCs := pvcs
	var snapName string
	primaryPVC := pvcs[0]
	for _, step := range pipeline {
		if step == "csiSnapshot" || step == "topolvmSnapshot" {
			if len(pvcs) != 1 {
				return r.fail(ctx, &backup, fmt.Errorf("csiSnapshot/topolvmSnapshot support a single PVC; use quiescedLive for multi-PVC"))
			}
			if backup.Spec.VolumeSnapshotClassName == "" {
				return r.fail(ctx, &backup, fmt.Errorf("volumeSnapshotClassName required"))
			}
			backup.Status.Phase = "Snapshotting"
			_ = r.Status().Update(ctx, &backup)
			snapName = fmt.Sprintf("%s-%d", backup.Name, started.Unix())
			logger.Info("creating volume snapshot", "snapshot", snapName, "pvc", primaryPVC, "class", backup.Spec.VolumeSnapshotClassName)
			if err := r.createSnapshot(ctx, &backup, snapName, primaryPVC); err != nil {
				return r.fail(ctx, &backup, err)
			}
			if err := r.waitSnapshotReady(ctx, backup.Namespace, snapName, 10*time.Minute); err != nil {
				return r.fail(ctx, &backup, err)
			}
			clone := fmt.Sprintf("%s-clone-%d", backup.Name, started.Unix())
			logger.Info("cloning PVC from snapshot", "clone", clone, "snapshot", snapName)
			if err := r.cloneFromSnapshot(ctx, &backup, snapName, clone, primaryPVC); err != nil {
				return r.fail(ctx, &backup, err)
			}
			uploadPVCs = []string{clone}
		}
		if step == "liveFlush" {
			logger.Info("liveFlush requested but not implemented yet; continuing pipeline")
		}
	}

	// Re-check before Job create: a restore may have claimed / interrupted us while we quiesced.
	var fresh operatorv1alpha1.PVCBackup
	if err := r.Get(ctx, types.NamespacedName{Name: backup.Name, Namespace: backup.Namespace}, &fresh); err == nil && fresh.Status.Phase == "Failed" {
		logger.Info("backup interrupted before job create; aborting")
		return ctrl.Result{}, nil
	}
	if busy, holder, err := pvcSessionBusy(ctx, r.Client, backupPVCKeys(&backup), "PVCBackup", backup.Namespace, backup.Name); err != nil {
		return ctrl.Result{}, err
	} else if busy {
		logger.Info("PVC session busy before job create; waiting", "holder", holder, "pvcs", pvcs)
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	backup.Status.Phase = "Uploading"
	jobName := fmt.Sprintf("pvcbackup-%s-%d", backup.Name, started.Unix())
	if len(jobName) > 63 {
		jobName = jobName[:63]
	}
	logger.Info("creating restic job", "job", jobName, "pvcs", uploadPVCs, "repoURL", repo.Spec.URL)
	if err := r.createResticBackupJob(ctx, &backup, &repo, jobName, uploadPVCs); err != nil {
		return r.fail(ctx, &backup, err)
	}
	// Keep workloads down even if later status patches conflict — Job already exists.
	holdQuiesceForJob = true
	if err := r.persistQuiesceState(ctx, &backup, quiesceState); err != nil {
		logger.Error(err, "persist quiesce state")
	}
	// Point status at the new Job in one write so a concurrent reconcile cannot
	// poll the previous Job name while Phase is already Uploading.
	backup.Status.LastJobName = jobName
	if err := r.Status().Update(ctx, &backup); err != nil {
		logger.Error(err, "status update after job create; will requeue to poll existing job")
		return ctrl.Result{Requeue: true}, nil
	}
	return r.pollBackupJob(ctx, &backup)
}

func (r *PVCBackupReconciler) pollBackupJob(ctx context.Context, backup *operatorv1alpha1.PVCBackup) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("pvcbackup", client.ObjectKeyFromObject(backup), "job", backup.Status.LastJobName)
	jobName := backup.Status.LastJobName
	var job batchv1.Job
	if err := r.Get(ctx, types.NamespacedName{Name: jobName, Namespace: backup.Namespace}, &job); err != nil {
		_ = r.releaseQuiesce(ctx, backup)
		return r.fail(ctx, backup, fmt.Errorf("restic job %s: %w", jobName, err))
	}
	if job.Status.Succeeded > 0 {
		logger.Info("restic job succeeded, restoring workloads")
		_ = r.releaseQuiesce(ctx, backup)
		if force := backup.Annotations[annForceRun]; force != "" {
			backup.Status.LastForceRun = force
		}
		backup.Status.Phase = "Succeeded"
		if backup.Spec.Schedule != "" {
			backup.Status.Phase = "Scheduled"
		}
		now := time.Now().UTC()
		ts := now.Format(time.RFC3339)
		backup.Status.LastBackupTime = ts
		backup.Status.LastSuccessTime = ts
		backup.Status.LastJobName = jobName
		metrics.ObserveBackupSuccess(backup.Namespace, backup.Name, float64(now.Unix()))
		logger.Info("backup completed", "lastBackupTime", backup.Status.LastBackupTime, "lastSuccessTime", backup.Status.LastSuccessTime)

		repoNS := backup.Spec.RepositoryRef.Namespace
		if repoNS == "" {
			repoNS = backup.Namespace
		}
		var repo operatorv1alpha1.BackupRepository
		if err := r.Get(ctx, types.NamespacedName{Name: backup.Spec.RepositoryRef.Name, Namespace: repoNS}, &repo); err == nil {
			if err := syncPVCBackupPlanToHost(ctx, r.Client, backup, &repo); err != nil {
				logger.Error(err, "sync plan/index to Backrest host after backup")
			}
		}

		if err := r.Status().Update(ctx, backup); err != nil {
			return ctrl.Result{}, err
		}
		if backup.Spec.Schedule != "" {
			_, wait, err := scheduleDue(backup)
			if err == nil && wait > 0 {
				return ctrl.Result{RequeueAfter: wait}, nil
			}
			return ctrl.Result{RequeueAfter: time.Hour}, nil
		}
		return ctrl.Result{}, nil
	}
	if job.Status.Failed > 0 {
		logger.Info("restic job failed, restoring workloads")
		_ = r.releaseQuiesce(ctx, backup)
		return r.fail(ctx, backup, fmt.Errorf("restic job failed"))
	}
	logger.V(1).Info("restic job still running")
	return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
}

func (r *PVCBackupReconciler) pollPruneJob(ctx context.Context, backup *operatorv1alpha1.PVCBackup) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("pvcbackup", client.ObjectKeyFromObject(backup), "job", backup.Status.LastJobName)
	jobName := backup.Status.LastJobName
	var job batchv1.Job
	if err := r.Get(ctx, types.NamespacedName{Name: jobName, Namespace: backup.Namespace}, &job); err != nil {
		return r.failPrune(ctx, backup, fmt.Errorf("restic prune job %s: %w", jobName, err))
	}
	if job.Status.Succeeded > 0 {
		logger.Info("restic prune job succeeded")
		backup.Status.Phase = idlePhaseAfterPrune(backup)
		backup.Status.LastJobName = jobName

		repoNS := backup.Spec.RepositoryRef.Namespace
		if repoNS == "" {
			repoNS = backup.Namespace
		}
		var repo operatorv1alpha1.BackupRepository
		if err := r.Get(ctx, types.NamespacedName{Name: backup.Spec.RepositoryRef.Name, Namespace: repoNS}, &repo); err == nil {
			if err := syncPVCBackupPlanToHost(ctx, r.Client, backup, &repo); err != nil {
				logger.Error(err, "sync plan/index to Backrest host after prune")
			}
		}

		if err := r.Status().Update(ctx, backup); err != nil {
			return ctrl.Result{}, err
		}
		if backup.Spec.Schedule != "" {
			_, wait, err := scheduleDue(backup)
			if err == nil && wait > 0 {
				return ctrl.Result{RequeueAfter: wait}, nil
			}
			return ctrl.Result{RequeueAfter: time.Hour}, nil
		}
		return ctrl.Result{}, nil
	}
	if job.Status.Failed > 0 {
		logger.Info("restic prune job failed")
		return r.failPrune(ctx, backup, fmt.Errorf("restic prune job failed"))
	}
	logger.V(1).Info("restic prune job still running")
	return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
}

func (r *PVCBackupReconciler) failPrune(ctx context.Context, b *operatorv1alpha1.PVCBackup, err error) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("pvcbackup", client.ObjectKeyFromObject(b))
	var cur operatorv1alpha1.PVCBackup
	if gerr := r.Get(ctx, types.NamespacedName{Name: b.Name, Namespace: b.Namespace}, &cur); gerr == nil && cur.Status.Phase == "Failed" {
		*b = cur
		if b.Spec.Schedule != "" {
			_, wait, serr := scheduleDue(b)
			if serr == nil && wait > 0 {
				return ctrl.Result{RequeueAfter: wait}, nil
			}
			return ctrl.Result{RequeueAfter: time.Hour}, nil
		}
		return ctrl.Result{}, nil
	}

	logger.Error(err, "prune failed")
	metrics.ReconcileErrors.WithLabelValues("PVCBackup").Inc()
	b.Status.Phase = "Failed"
	b.Status.Conditions = []operatorv1alpha1.Condition{{Type: "Failed", Status: "True", Message: err.Error()}}
	_ = r.Status().Update(ctx, b)
	if b.Spec.Schedule != "" {
		_, wait, serr := scheduleDue(b)
		if serr == nil && wait > 0 {
			return ctrl.Result{RequeueAfter: wait}, nil
		}
		return ctrl.Result{RequeueAfter: time.Hour}, nil
	}
	return ctrl.Result{}, nil
}

func (r *PVCBackupReconciler) persistQuiesceState(ctx context.Context, b *operatorv1alpha1.PVCBackup, state map[string]int32) error {
	if len(state) == 0 {
		return nil
	}
	raw, err := jsonMarshal(state)
	if err != nil {
		return err
	}
	var cur operatorv1alpha1.PVCBackup
	if err := r.Get(ctx, types.NamespacedName{Name: b.Name, Namespace: b.Namespace}, &cur); err != nil {
		return err
	}
	if cur.Annotations == nil {
		cur.Annotations = map[string]string{}
	}
	cur.Annotations[annQuiesceState] = string(raw)
	return r.Update(ctx, &cur)
}

func (r *PVCBackupReconciler) releaseQuiesce(ctx context.Context, b *operatorv1alpha1.PVCBackup) error {
	if b.Spec.Quiesce.LeaveDown {
		return r.clearQuiesceAnnotation(ctx, b)
	}
	state := map[string]int32{}
	if raw := b.Annotations[annQuiesceState]; raw != "" {
		_ = jsonUnmarshal([]byte(raw), &state)
	}
	if len(state) == 0 {
		// Fallback: scale targets back to 1.
		for _, t := range b.Spec.Quiesce.Targets {
			ns := t.Namespace
			if ns == "" {
				ns = b.Namespace
			}
			state[t.Kind+"/"+ns+"/"+t.Name] = 1
		}
	}
	if err := r.unquiesce(ctx, b.Namespace, state); err != nil {
		return err
	}
	return r.clearQuiesceAnnotation(ctx, b)
}

func (r *PVCBackupReconciler) clearQuiesceAnnotation(ctx context.Context, b *operatorv1alpha1.PVCBackup) error {
	var cur operatorv1alpha1.PVCBackup
	if err := r.Get(ctx, types.NamespacedName{Name: b.Name, Namespace: b.Namespace}, &cur); err != nil {
		return err
	}
	if cur.Annotations == nil || cur.Annotations[annQuiesceState] == "" {
		return nil
	}
	delete(cur.Annotations, annQuiesceState)
	return r.Update(ctx, &cur)
}

func pvcList(b *operatorv1alpha1.PVCBackup) []string {
	if len(b.Spec.PVCNames) > 0 {
		return append([]string{}, b.Spec.PVCNames...)
	}
	if b.Spec.PVCName != "" {
		return []string{b.Spec.PVCName}
	}
	return nil
}

func forceRunPending(b *operatorv1alpha1.PVCBackup) bool {
	force := b.Annotations[annForceRun]
	return force != "" && force != b.Status.LastForceRun
}

func forcePrunePending(b *operatorv1alpha1.PVCBackup) bool {
	force := b.Annotations[annForcePrune]
	return force != "" && force != b.Status.LastForcePrune
}

func backupPhaseInFlight(phase string) bool {
	switch phase {
	case "Pending", "Quiescing", "Snapshotting", "Uploading":
		return true
	default:
		return false
	}
}

func idlePhaseAfterPrune(b *operatorv1alpha1.PVCBackup) string {
	if b.Spec.Schedule != "" {
		return "Scheduled"
	}
	return "Succeeded"
}

func pruneJobScript(b *operatorv1alpha1.PVCBackup) string {
	planID := planIDForPVCBackup(b)
	planTag := backrest.PlanTag(planID)
	keepLast := *b.Spec.Retention.KeepLast
	return fmt.Sprintf("restic unlock || true; restic forget --retry-lock 5m --group-by tags --tag %s --keep-last %d --prune || true",
		shellQuoteOne(planTag), keepLast)
}

func scheduleDue(b *operatorv1alpha1.PVCBackup) (due bool, wait time.Duration, err error) {
	if forceRunPending(b) {
		return true, 0, nil
	}
	sched, err := cron.ParseStandard(b.Spec.Schedule)
	if err != nil {
		return false, 0, fmt.Errorf("invalid schedule %q: %w", b.Spec.Schedule, err)
	}
	from := b.CreationTimestamp.Time
	if b.Status.LastBackupTime != "" {
		if t, perr := time.Parse(time.RFC3339, b.Status.LastBackupTime); perr == nil {
			from = t
		}
	}
	// After a failed run LastBackupTime is the attempt time; Next(from) is the following slot.
	next := sched.Next(from)
	now := time.Now()
	if now.Before(next) {
		return false, next.Sub(now), nil
	}
	return true, 0, nil
}

func (r *PVCBackupReconciler) cleanupLegacyScheduleResources(ctx context.Context, b *operatorv1alpha1.PVCBackup) error {
	name := "pvcbackup-" + b.Name
	if len(name) > 63 {
		name = name[:63]
	}
	ns := b.Namespace
	_ = client.IgnoreNotFound(r.Delete(ctx, &batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}}))
	_ = client.IgnoreNotFound(r.Delete(ctx, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}}))
	_ = client.IgnoreNotFound(r.Delete(ctx, &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}}))
	_ = client.IgnoreNotFound(r.Delete(ctx, &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}}))
	return nil
}

func (r *PVCBackupReconciler) ensureRepoSecrets(ctx context.Context, b *operatorv1alpha1.PVCBackup, repo *operatorv1alpha1.BackupRepository) error {
	repoNS := repo.Namespace
	if repoNS == b.Namespace {
		return nil
	}
	if err := r.mirrorSecret(ctx, repoNS, repo.Spec.PasswordSecretRef.Name, b.Namespace, repo.Spec.PasswordSecretRef.Name, b); err != nil {
		return err
	}
	if repo.Spec.EnvFromSecretRef != nil && repo.Spec.EnvFromSecretRef.Name != "" {
		if err := r.mirrorSecret(ctx, repoNS, repo.Spec.EnvFromSecretRef.Name, b.Namespace, repo.Spec.EnvFromSecretRef.Name, b); err != nil {
			return err
		}
	}
	return nil
}

func (r *PVCBackupReconciler) mirrorSecret(ctx context.Context, srcNS, srcName, dstNS, dstName string, owner client.Object) error {
	var src corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Namespace: srcNS, Name: srcName}, &src); err != nil {
		return err
	}
	desired := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: dstName, Namespace: dstNS},
		Type:       src.Type,
		Data:       src.Data,
	}
	_ = controllerutil.SetControllerReference(owner, desired, r.Scheme)
	var cur corev1.Secret
	err := r.Get(ctx, client.ObjectKeyFromObject(desired), &cur)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	cur.Data = src.Data
	cur.Type = src.Type
	return r.Update(ctx, &cur)
}

func (r *PVCBackupReconciler) fail(ctx context.Context, b *operatorv1alpha1.PVCBackup, err error) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("pvcbackup", client.ObjectKeyFromObject(b))
	// Idempotent: restore preempt or a racing poll may call fail twice.
	var cur operatorv1alpha1.PVCBackup
	if gerr := r.Get(ctx, types.NamespacedName{Name: b.Name, Namespace: b.Namespace}, &cur); gerr == nil && cur.Status.Phase == "Failed" {
		_ = r.releaseQuiesce(ctx, &cur)
		*b = cur
		if b.Spec.Schedule != "" {
			_, wait, serr := scheduleDue(b)
			if serr == nil && wait > 0 {
				return ctrl.Result{RequeueAfter: wait}, nil
			}
			return ctrl.Result{RequeueAfter: time.Hour}, nil
		}
		return ctrl.Result{}, nil
	}

	logger.Error(err, "backup failed")
	metrics.ReconcileErrors.WithLabelValues("PVCBackup").Inc()
	metrics.ObserveBackupFailure(b.Namespace, b.Name)
	// Always try to bring workloads back after a failed backup (unless leaveDown).
	_ = r.releaseQuiesce(ctx, b)
	b.Status.Phase = "Failed"
	// Advance the schedule cursor so a missed/failed window is not retried every reconcile.
	b.Status.LastBackupTime = time.Now().UTC().Format(time.RFC3339)
	b.Status.Conditions = []operatorv1alpha1.Condition{{Type: "Failed", Status: "True", Message: err.Error()}}
	_ = r.Status().Update(ctx, b)
	if b.Spec.Schedule != "" {
		_, wait, serr := scheduleDue(b)
		if serr == nil && wait > 0 {
			return ctrl.Result{RequeueAfter: wait}, nil
		}
		return ctrl.Result{RequeueAfter: time.Hour}, nil
	}
	// Do not return err: controller-runtime would requeue immediately and storm Jobs.
	return ctrl.Result{}, nil
}

func jobRetries(b *operatorv1alpha1.PVCBackup) int32 {
	if b.Spec.Retries != nil {
		return *b.Spec.Retries
	}
	if b.Spec.BackoffLimit != nil {
		return *b.Spec.BackoffLimit
	}
	return 0
}

func (r *PVCBackupReconciler) findOwnedBackupJob(ctx context.Context, b *operatorv1alpha1.PVCBackup) (string, bool) {
	var jobs batchv1.JobList
	if err := r.List(ctx, &jobs, client.InNamespace(b.Namespace)); err != nil {
		return "", false
	}
	prefix := "pvcbackup-" + b.Name + "-"
	var newest string
	var newestTS int64
	for i := range jobs.Items {
		j := &jobs.Items[i]
		if !strings.HasPrefix(j.Name, prefix) {
			continue
		}
		if !metav1.IsControlledBy(j, b) {
			continue
		}
		// Skip finished jobs — only adopt in-flight work.
		if j.Status.Succeeded > 0 || j.Status.Failed > 0 {
			continue
		}
		ts := j.CreationTimestamp.Unix()
		if ts >= newestTS {
			newestTS = ts
			newest = j.Name
		}
	}
	if newest == "" {
		return "", false
	}
	return newest, true
}

func (r *PVCBackupReconciler) findOwnedPruneJob(ctx context.Context, b *operatorv1alpha1.PVCBackup) (string, bool) {
	var jobs batchv1.JobList
	if err := r.List(ctx, &jobs, client.InNamespace(b.Namespace)); err != nil {
		return "", false
	}
	prefix := "pvcbackup-prune-" + b.Name + "-"
	var newest string
	var newestTS int64
	for i := range jobs.Items {
		j := &jobs.Items[i]
		if !strings.HasPrefix(j.Name, prefix) {
			continue
		}
		if !metav1.IsControlledBy(j, b) {
			continue
		}
		if j.Status.Succeeded > 0 || j.Status.Failed > 0 {
			continue
		}
		ts := j.CreationTimestamp.Unix()
		if ts >= newestTS {
			newestTS = ts
			newest = j.Name
		}
	}
	if newest == "" {
		return "", false
	}
	return newest, true
}

func (r *PVCBackupReconciler) quiesce(ctx context.Context, b *operatorv1alpha1.PVCBackup) (map[string]int32, error) {
	state := map[string]int32{}
	for _, t := range b.Spec.Quiesce.Targets {
		ns := t.Namespace
		if ns == "" {
			ns = b.Namespace
		}
		prev, err := r.scaleWorkload(ctx, t.Kind, ns, t.Name, 0)
		if err != nil {
			return state, err
		}
		state[t.Kind+"/"+ns+"/"+t.Name] = prev
	}
	return state, nil
}

func (r *PVCBackupReconciler) unquiesce(ctx context.Context, defaultNS string, state map[string]int32) error {
	for key, replicas := range state {
		parts := split3(key)
		if len(parts) != 3 {
			continue
		}
		_, _ = r.scaleWorkload(ctx, parts[0], parts[1], parts[2], replicas)
	}
	return nil
}

func split3(s string) []string {
	return strings.SplitN(s, "/", 3)
}

func (r *PVCBackupReconciler) scaleWorkload(ctx context.Context, kind, ns, name string, replicas int32) (int32, error) {
	switch {
	case strings.EqualFold(kind, "Pod"):
		if replicas > 0 {
			// Unquiesce: workload controller recreates the pod.
			return replicas, nil
		}
		key := types.NamespacedName{Name: name, Namespace: ns}
		var pod corev1.Pod
		if err := r.Get(ctx, key, &pod); err != nil {
			if apierrors.IsNotFound(err) {
				return 0, nil
			}
			return 0, err
		}
		if err := r.Delete(ctx, &pod); err != nil && !apierrors.IsNotFound(err) {
			return 1, err
		}
		if err := r.waitPodDeleted(ctx, ns, name, podDeleteTimeout); err != nil {
			return 1, err
		}
		return 1, nil
	case kind == "StatefulSet":
		u := &unstructured.Unstructured{}
		u.SetGroupVersionKind(schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "StatefulSet"})
		if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, u); err != nil {
			return 0, err
		}
		prev, _, _ := unstructured.NestedInt64(u.Object, "spec", "replicas")
		if err := unstructured.SetNestedField(u.Object, int64(replicas), "spec", "replicas"); err != nil {
			return 0, err
		}
		return int32(prev), r.Update(ctx, u)
	case kind == "Deployment":
		u := &unstructured.Unstructured{}
		u.SetGroupVersionKind(schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"})
		if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, u); err != nil {
			return 0, err
		}
		prev, _, _ := unstructured.NestedInt64(u.Object, "spec", "replicas")
		if err := unstructured.SetNestedField(u.Object, int64(replicas), "spec", "replicas"); err != nil {
			return 0, err
		}
		return int32(prev), r.Update(ctx, u)
	default:
		return 0, fmt.Errorf("unsupported quiesce kind %s (supported: StatefulSet, Deployment, Pod)", kind)
	}
}

func (r *PVCBackupReconciler) waitPodDeleted(ctx context.Context, ns, name string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	key := types.NamespacedName{Name: name, Namespace: ns}
	for time.Now().Before(deadline) {
		var pod corev1.Pod
		err := r.Get(ctx, key, &pod)
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if pod.DeletionTimestamp == nil {
			// Pod reappeared or delete not yet visible; re-issue delete.
			if derr := r.Delete(ctx, &pod); derr != nil && !apierrors.IsNotFound(derr) {
				return derr
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return fmt.Errorf("pod %s/%s not deleted within %s", ns, name, timeout)
}

func (r *PVCBackupReconciler) createSnapshot(ctx context.Context, b *operatorv1alpha1.PVCBackup, snapName, pvcName string) error {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(volumeSnapshotGVK)
	u.SetName(snapName)
	u.SetNamespace(b.Namespace)
	u.Object["spec"] = map[string]interface{}{
		"volumeSnapshotClassName": b.Spec.VolumeSnapshotClassName,
		"source": map[string]interface{}{
			"persistentVolumeClaimName": pvcName,
		},
	}
	err := r.Create(ctx, u)
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func (r *PVCBackupReconciler) waitSnapshotReady(ctx context.Context, ns, name string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		u := &unstructured.Unstructured{}
		u.SetGroupVersionKind(volumeSnapshotGVK)
		if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, u); err != nil {
			return err
		}
		ready, found, _ := unstructured.NestedBool(u.Object, "status", "readyToUse")
		if found && ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
	return fmt.Errorf("volume snapshot %s not ReadyToUse within %s", name, timeout)
}

func (r *PVCBackupReconciler) deleteSnapshot(ctx context.Context, ns, name string) error {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(volumeSnapshotGVK)
	u.SetName(name)
	u.SetNamespace(ns)
	return client.IgnoreNotFound(r.Delete(ctx, u))
}

func (r *PVCBackupReconciler) cloneFromSnapshot(ctx context.Context, b *operatorv1alpha1.PVCBackup, snapName, cloneName, srcPVC string) error {
	var src corev1.PersistentVolumeClaim
	if err := r.Get(ctx, types.NamespacedName{Name: srcPVC, Namespace: b.Namespace}, &src); err != nil {
		return err
	}
	clone := src.DeepCopy()
	clone.ObjectMeta = metav1.ObjectMeta{Name: cloneName, Namespace: b.Namespace}
	clone.Spec.VolumeName = ""
	clone.Spec.DataSource = &corev1.TypedLocalObjectReference{
		APIGroup: strPtr("snapshot.storage.k8s.io"),
		Kind:     "VolumeSnapshot",
		Name:     snapName,
	}
	clone.Status = corev1.PersistentVolumeClaimStatus{}
	err := r.Create(ctx, clone)
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func strPtr(s string) *string { return &s }

// buildResticBackupJobVolumes builds PVC volumes/mounts for the restic backup Job.
// Mounts are read-write so kubelet/CSI (e.g. TopoLVM) can chmod the mount point during SetUp.
func buildResticBackupJobVolumes(b *operatorv1alpha1.PVCBackup, pvcNames []string) ([]corev1.Volume, []corev1.VolumeMount, []string) {
	vols := []corev1.Volume{}
	mounts := []corev1.VolumeMount{}
	paths := b.Spec.Paths
	if len(paths) == 1 && (paths[0] == "/" || paths[0] == "") {
		paths = nil
	}
	if len(paths) == 0 {
		paths = nil
		for i, pvc := range pvcNames {
			volName := fmt.Sprintf("data-%d", i)
			mountPath := "/data/" + sanitizePath(pvc)
			vols = append(vols, corev1.Volume{
				Name: volName,
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc, ReadOnly: false},
				},
			})
			mounts = append(mounts, corev1.VolumeMount{Name: volName, MountPath: mountPath, ReadOnly: false})
			paths = append(paths, mountPath)
		}
	} else {
		for i, pvc := range pvcNames {
			volName := fmt.Sprintf("data-%d", i)
			mount := paths[0]
			if i < len(paths) {
				mount = paths[i]
			}
			vols = append(vols, corev1.Volume{
				Name: volName,
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc, ReadOnly: false},
				},
			})
			mounts = append(mounts, corev1.VolumeMount{Name: volName, MountPath: mount, ReadOnly: false})
		}
	}
	return vols, mounts, paths
}

func (r *PVCBackupReconciler) createResticBackupJob(ctx context.Context, b *operatorv1alpha1.PVCBackup, repo *operatorv1alpha1.BackupRepository, jobName string, pvcNames []string) error {
	enableLinks := false
	backoff := jobRetries(b)
	ttl := int32(86400)
	if b.Spec.TTLSecondsAfterFinished != nil {
		ttl = *b.Spec.TTLSecondsAfterFinished
	}

	vols, mounts, paths := buildResticBackupJobVolumes(b, pvcNames)

	// Unlock stale locks, backup with lock retry; retention must not fail a successful backup.
	script := "restic unlock || true; restic snapshots >/dev/null 2>&1 || restic init; restic backup --retry-lock 5m " + strings.Join(shellQuote(paths), " ")
	for _, ex := range b.Spec.Excludes {
		script += " --exclude " + shellQuoteOne(ex)
	}
	// Tag so Backrest UI associates snapshots with the synced plan/instance.
	planID := planIDForPVCBackup(b)
	instance := "main"
	if b.Spec.RepositoryRef.Namespace != "" || b.Spec.RepositoryRef.Name != "" {
		// Prefer BackrestCluster name from repo sync target when available at Job create time.
		var repo operatorv1alpha1.BackupRepository
		repoNS := b.Spec.RepositoryRef.Namespace
		if repoNS == "" {
			repoNS = b.Namespace
		}
		if err := r.Get(ctx, types.NamespacedName{Name: b.Spec.RepositoryRef.Name, Namespace: repoNS}, &repo); err == nil {
			_, clusterName := resolveClusterRef(repo.Spec.Backrest.ClusterRef, repo.Namespace)
			instance = instanceForCluster(clusterName)
		}
	}
	script += " --tag " + shellQuoteOne(backrest.PlanTag(planID))
	script += " --tag " + shellQuoteOne(backrest.InstanceTag(instance))
	if b.Spec.Retention.KeepLast != nil {
		// Group by tags only: Job pods use unique hostnames, so default host grouping would keep everything.
		script += fmt.Sprintf("; restic forget --retry-lock 5m --group-by tags --tag %s --keep-last %d --prune || true", shellQuoteOne(backrest.PlanTag(planID)), *b.Spec.Retention.KeepLast)
	}
	cmd := []string{"sh", "-ec", script}

	env := resticEnv(repo)
	container := corev1.Container{
		Name:         "restic",
		Image:        resticImage,
		Command:      cmd,
		Env:          env,
		VolumeMounts: mounts,
	}
	if repo.Spec.EnvFromSecretRef != nil && repo.Spec.EnvFromSecretRef.Name != "" {
		container.EnvFrom = []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: repo.Spec.EnvFromSecretRef.Name},
		}}}
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: jobName, Namespace: b.Namespace},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoff,
			TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy:      corev1.RestartPolicyNever,
					EnableServiceLinks: &enableLinks,
					NodeSelector:       b.Spec.NodeSelector,
					Containers:         []corev1.Container{container},
					Volumes:            vols,
				},
			},
		},
	}
	_ = controllerutil.SetControllerReference(b, job, r.Scheme)
	err := r.Create(ctx, job)
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func buildResticPruneJob(b *operatorv1alpha1.PVCBackup, repo *operatorv1alpha1.BackupRepository, jobName string) *batchv1.Job {
	enableLinks := false
	backoff := jobRetries(b)
	ttl := int32(86400)
	if b.Spec.TTLSecondsAfterFinished != nil {
		ttl = *b.Spec.TTLSecondsAfterFinished
	}

	cmd := []string{"sh", "-ec", pruneJobScript(b)}
	env := resticEnv(repo)
	container := corev1.Container{
		Name:    "restic",
		Image:   resticImage,
		Command: cmd,
		Env:     env,
	}
	if repo.Spec.EnvFromSecretRef != nil && repo.Spec.EnvFromSecretRef.Name != "" {
		container.EnvFrom = []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: repo.Spec.EnvFromSecretRef.Name},
		}}}
	}
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: jobName, Namespace: b.Namespace},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoff,
			TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy:      corev1.RestartPolicyNever,
					EnableServiceLinks: &enableLinks,
					NodeSelector:       b.Spec.NodeSelector,
					Containers:         []corev1.Container{container},
				},
			},
		},
	}
}

func (r *PVCBackupReconciler) createResticPruneJob(ctx context.Context, b *operatorv1alpha1.PVCBackup, repo *operatorv1alpha1.BackupRepository, jobName string) error {
	job := buildResticPruneJob(b, repo, jobName)
	_ = controllerutil.SetControllerReference(b, job, r.Scheme)
	err := r.Create(ctx, job)
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func sanitizePath(s string) string {
	s = strings.ReplaceAll(s, "/", "-")
	if s == "" {
		return "vol"
	}
	return s
}

func shellQuote(paths []string) []string {
	out := make([]string, len(paths))
	for i, p := range paths {
		out[i] = shellQuoteOne(p)
	}
	return out
}

func shellQuoteOne(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'"'"'`) + "'"
}

func resticEnv(repo *operatorv1alpha1.BackupRepository) []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: "RESTIC_REPOSITORY", Value: repo.Spec.URL},
		{Name: "RESTIC_PASSWORD", ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: repo.Spec.PasswordSecretRef.Name},
				Key:                  keyOr(repo.Spec.PasswordSecretRef.Key, "RESTIC_PASSWORD"),
			},
		}},
	}
}

func (r *PVCBackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&operatorv1alpha1.PVCBackup{}).
		Owns(&batchv1.Job{}).
		Complete(r)
}
