package controller

import (
	"context"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	operatorv1alpha1 "github.com/Reactive-Network/backrest-operator/api/v1alpha1"
	"github.com/Reactive-Network/backrest-operator/internal/filters"
	"github.com/Reactive-Network/backrest-operator/internal/metrics"
)

const annRestoreQuiesceState = "operator.backrest.io/restore-quiesce-state"

type PVCRestoreReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

func (r *PVCRestoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("pvcrestore", req.NamespacedName)
	var restore operatorv1alpha1.PVCRestore
	if err := r.Get(ctx, req.NamespacedName, &restore); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !filters.ObjectAllowed(restore.Namespace, restore.Labels) {
		logger.V(1).Info("skipped by watch filter")
		return ctrl.Result{}, nil
	}
	switch restore.Status.Phase {
	case "Succeeded", "Failed":
		return ctrl.Result{}, nil
	case "Restoring":
		if restore.Status.LastJobName != "" {
			return r.pollRestoreJob(ctx, &restore)
		}
	}

	mode := restore.Spec.Mode
	logger.Info("restore started", "mode", mode)
	switch mode {
	case "fromVolumeSnapshot":
		if err := r.restoreFromSnapshot(ctx, &restore); err != nil {
			return r.fail(ctx, &restore, err)
		}
		restore.Status.Phase = "Succeeded"
		return ctrl.Result{}, r.Status().Update(ctx, &restore)
	case "fromResticToNewPVC", "fromResticToExistingPVC":
		return r.startResticRestore(ctx, &restore)
	case "export":
		return r.fail(ctx, &restore, fmt.Errorf("mode export is removed; use Backrest GetDownloadURL / MCP get_snapshot_download_url"))
	default:
		return r.fail(ctx, &restore, fmt.Errorf("unknown mode %s", mode))
	}
}

func (r *PVCRestoreReconciler) fail(ctx context.Context, restore *operatorv1alpha1.PVCRestore, err error) (ctrl.Result, error) {
	log.FromContext(ctx).Error(err, "restore failed", "pvcrestore", client.ObjectKeyFromObject(restore))
	metrics.ReconcileErrors.WithLabelValues("PVCRestore").Inc()
	metrics.ObserveRestoreFailure(restore.Namespace, restore.Name)
	// On restore failure: alert via metrics, do NOT restart quiesced workloads.
	restore.Status.Phase = "Failed"
	restore.Status.Conditions = []operatorv1alpha1.Condition{{Type: "Failed", Status: "True", Message: err.Error()}}
	_ = r.Status().Update(ctx, restore)
	return ctrl.Result{}, nil
}

func (r *PVCRestoreReconciler) restoreFromSnapshot(ctx context.Context, restore *operatorv1alpha1.PVCRestore) error {
	name := restore.Spec.Target.NewPVC.Name
	if name == "" {
		name = restore.Name + "-pvc"
	}
	size := restore.Spec.Target.NewPVC.Size
	if size == "" {
		size = "10Gi"
	}
	apiGroup := "snapshot.storage.k8s.io"
	modes := []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: restore.Namespace},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: modes,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(size)},
			},
			DataSource: &corev1.TypedLocalObjectReference{
				APIGroup: &apiGroup,
				Kind:     "VolumeSnapshot",
				Name:     restore.Spec.VolumeSnapshotRef.Name,
			},
		},
	}
	if sc := restore.Spec.Target.NewPVC.StorageClassName; sc != "" {
		pvc.Spec.StorageClassName = &sc
	}
	err := r.Create(ctx, pvc)
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func (r *PVCRestoreReconciler) startResticRestore(ctx context.Context, restore *operatorv1alpha1.PVCRestore) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("pvcrestore", client.ObjectKeyFromObject(restore))
	repoNS := restore.Spec.RepositoryRef.Namespace
	if repoNS == "" {
		repoNS = restore.Namespace
	}
	var repo operatorv1alpha1.BackupRepository
	if err := r.Get(ctx, types.NamespacedName{Name: restore.Spec.RepositoryRef.Name, Namespace: repoNS}, &repo); err != nil {
		return r.fail(ctx, restore, err)
	}
	// PVC mount session: same disk → one Job; restore preempts backup; other disks / archives unlocked.
	sessionKeys := restorePVCKeys(restore)
	holders, err := listPVCSessionHolders(ctx, r.Client, sessionKeys, "PVCRestore", restore.Namespace, restore.Name)
	if err != nil {
		return ctrl.Result{}, err
	}
	if restores := holdersOfKind(holders, "PVCRestore"); len(restores) > 0 {
		logger.Info("PVC session busy; waiting for other restore", "holder", formatHolders(restores), "pvcs", sessionKeys)
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	for _, h := range holdersOfKind(holders, "PVCBackup") {
		var b operatorv1alpha1.PVCBackup
		if err := r.Get(ctx, types.NamespacedName{Name: h.Name, Namespace: h.Namespace}, &b); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return ctrl.Result{}, err
		}
		if !backupPhaseActive(b.Status.Phase) {
			continue
		}
		logger.Info("interrupting backup for restore", "pvcbackup", h.Namespace+"/"+h.Name, "phase", b.Status.Phase, "pvcs", sessionKeys)
		if err := interruptBackup(ctx, r.Client, r.Scheme, &b, fmt.Sprintf("interrupted by PVCRestore/%s/%s (same PVC mount)", restore.Namespace, restore.Name)); err != nil {
			return ctrl.Result{}, err
		}
	}

	var pvcName string
	br := &PVCBackupReconciler{Client: r.Client, Scheme: r.Scheme}
	if restore.Spec.Mode == "fromResticToNewPVC" {
		pvcName = restore.Spec.Target.NewPVC.Name
		if pvcName == "" {
			pvcName = restore.Name + "-pvc"
		}
		size := restore.Spec.Target.NewPVC.Size
		if size == "" {
			size = "10Gi"
		}
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: pvcName, Namespace: restore.Namespace},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(size)},
				},
			},
		}
		if sc := restore.Spec.Target.NewPVC.StorageClassName; sc != "" {
			pvc.Spec.StorageClassName = &sc
		}
		if err := r.Create(ctx, pvc); err != nil && !apierrors.IsAlreadyExists(err) {
			return r.fail(ctx, restore, err)
		}
	} else {
		pvcName = restore.Spec.Target.ExistingPVCName
		if pvcName == "" {
			return r.fail(ctx, restore, fmt.Errorf("target.existingPVCName required"))
		}
		if restore.Spec.Quiesce.Enabled {
			restore.Status.Phase = "Quiescing"
			_ = r.Status().Update(ctx, restore)
			fake := &operatorv1alpha1.PVCBackup{
				ObjectMeta: metav1.ObjectMeta{Namespace: restore.Namespace},
				Spec:       operatorv1alpha1.PVCBackupSpec{Quiesce: restore.Spec.Quiesce},
			}
			quiesceState, err := br.quiesce(ctx, fake)
			if err != nil {
				return r.fail(ctx, restore, err)
			}
			if err := r.persistRestoreQuiesce(ctx, restore, quiesceState); err != nil {
				return r.fail(ctx, restore, err)
			}
		}
	}

	snap := restore.Spec.Restic.SnapshotID
	if snap == "" {
		snap = "latest"
	}
	if err := r.ensureRestoreRepoSecrets(ctx, restore, &repo, repoNS); err != nil {
		return r.fail(ctx, restore, err)
	}
	cmd := []string{"sh", "-ec", resticRestoreJobScript(snap, restore.Spec.Restic.PathFilters)}
	jobName := fmt.Sprintf("pvcrestore-%s-%d", restore.Name, time.Now().Unix())
	if len(jobName) > 63 {
		jobName = jobName[:63]
	}
	enableLinks := false
	zero := int32(0)
	resticContainer := corev1.Container{
		Name:    "restic",
		Image:   resticImage,
		Command: cmd,
		Env:     resticEnv(&repo),
		VolumeMounts: []corev1.VolumeMount{{
			Name: "data", MountPath: "/data",
		}},
	}
	if repo.Spec.EnvFromSecretRef != nil && repo.Spec.EnvFromSecretRef.Name != "" {
		resticContainer.EnvFrom = []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: repo.Spec.EnvFromSecretRef.Name},
		}}}
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: jobName, Namespace: restore.Namespace},
		Spec: batchv1.JobSpec{
			BackoffLimit: &zero,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy:      corev1.RestartPolicyNever,
					EnableServiceLinks: &enableLinks,
					Containers:         []corev1.Container{resticContainer},
					Volumes: []corev1.Volume{{
						Name: "data",
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvcName},
						},
					}},
				},
			},
		},
	}
	if err := r.Create(ctx, job); err != nil && !apierrors.IsAlreadyExists(err) {
		return r.fail(ctx, restore, err)
	}
	restore.Status.LastJobName = jobName
	restore.Status.Phase = "Restoring"
	if err := r.Status().Update(ctx, restore); err != nil {
		return ctrl.Result{}, err
	}
	return r.pollRestoreJob(ctx, restore)
}

func (r *PVCRestoreReconciler) pollRestoreJob(ctx context.Context, restore *operatorv1alpha1.PVCRestore) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("pvcrestore", client.ObjectKeyFromObject(restore), "job", restore.Status.LastJobName)
	var job batchv1.Job
	if err := r.Get(ctx, types.NamespacedName{Name: restore.Status.LastJobName, Namespace: restore.Namespace}, &job); err != nil {
		return r.fail(ctx, restore, fmt.Errorf("restic restore job %s: %w", restore.Status.LastJobName, err))
	}
	if job.Status.Succeeded > 0 {
		logger.Info("restic restore succeeded; unquiescing workloads")
		_ = r.releaseRestoreQuiesce(ctx, restore)
		restore.Status.Phase = "Succeeded"
		return ctrl.Result{}, r.Status().Update(ctx, restore)
	}
	if job.Status.Failed > 0 {
		logger.Info("restic restore failed; leaving workloads down")
		return r.fail(ctx, restore, fmt.Errorf("restic restore job failed"))
	}
	return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
}

func (r *PVCRestoreReconciler) persistRestoreQuiesce(ctx context.Context, restore *operatorv1alpha1.PVCRestore, state map[string]int32) error {
	if len(state) == 0 {
		return nil
	}
	raw, err := jsonMarshal(state)
	if err != nil {
		return err
	}
	var cur operatorv1alpha1.PVCRestore
	if err := r.Get(ctx, types.NamespacedName{Name: restore.Name, Namespace: restore.Namespace}, &cur); err != nil {
		return err
	}
	if cur.Annotations == nil {
		cur.Annotations = map[string]string{}
	}
	cur.Annotations[annRestoreQuiesceState] = string(raw)
	if err := r.Update(ctx, &cur); err != nil {
		return err
	}
	restore.Annotations = cur.Annotations
	return nil
}

func (r *PVCRestoreReconciler) releaseRestoreQuiesce(ctx context.Context, restore *operatorv1alpha1.PVCRestore) error {
	if restore.Spec.Quiesce.LeaveDown {
		return r.clearRestoreQuiesceAnnotation(ctx, restore)
	}
	state := map[string]int32{}
	if raw := restore.Annotations[annRestoreQuiesceState]; raw != "" {
		_ = jsonUnmarshal([]byte(raw), &state)
	}
	if len(state) == 0 {
		for _, t := range restore.Spec.Quiesce.Targets {
			ns := t.Namespace
			if ns == "" {
				ns = restore.Namespace
			}
			state[t.Kind+"/"+ns+"/"+t.Name] = 1
		}
	}
	br := &PVCBackupReconciler{Client: r.Client, Scheme: r.Scheme}
	if err := br.unquiesce(ctx, restore.Namespace, state); err != nil {
		return err
	}
	return r.clearRestoreQuiesceAnnotation(ctx, restore)
}

func (r *PVCRestoreReconciler) clearRestoreQuiesceAnnotation(ctx context.Context, restore *operatorv1alpha1.PVCRestore) error {
	var cur operatorv1alpha1.PVCRestore
	if err := r.Get(ctx, types.NamespacedName{Name: restore.Name, Namespace: restore.Namespace}, &cur); err != nil {
		return err
	}
	if cur.Annotations == nil || cur.Annotations[annRestoreQuiesceState] == "" {
		return nil
	}
	delete(cur.Annotations, annRestoreQuiesceState)
	return r.Update(ctx, &cur)
}

func (r *PVCRestoreReconciler) ensureRestoreRepoSecrets(ctx context.Context, restore *operatorv1alpha1.PVCRestore, repo *operatorv1alpha1.BackupRepository, repoNS string) error {
	if repoNS == restore.Namespace {
		return nil
	}
	if err := r.mirrorSecret(ctx, repoNS, repo.Spec.PasswordSecretRef.Name, restore.Namespace, repo.Spec.PasswordSecretRef.Name); err != nil {
		return err
	}
	if repo.Spec.EnvFromSecretRef != nil && repo.Spec.EnvFromSecretRef.Name != "" {
		if err := r.mirrorSecret(ctx, repoNS, repo.Spec.EnvFromSecretRef.Name, restore.Namespace, repo.Spec.EnvFromSecretRef.Name); err != nil {
			return err
		}
	}
	return nil
}

func (r *PVCRestoreReconciler) mirrorSecret(ctx context.Context, srcNS, srcName, dstNS, dstName string) error {
	var src corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Namespace: srcNS, Name: srcName}, &src); err != nil {
		return err
	}
	desired := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: dstName, Namespace: dstNS},
		Type:       src.Type,
		Data:       src.Data,
	}
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

func (r *PVCRestoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&operatorv1alpha1.PVCRestore{}).
		Owns(&batchv1.Job{}).
		Complete(r)
}
