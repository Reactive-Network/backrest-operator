package controller

import (
	"context"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	operatorv1alpha1 "github.com/Reactive-Network/backrest-operator/api/v1alpha1"
	"github.com/Reactive-Network/backrest-operator/internal/filters"
	"github.com/Reactive-Network/backrest-operator/internal/metrics"
)

const resticImage = "restic/restic:0.19.1"

type BackupRepositoryReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

func (r *BackupRepositoryReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("backuprepository", req.NamespacedName)
	var repo operatorv1alpha1.BackupRepository
	if err := r.Get(ctx, req.NamespacedName, &repo); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !filters.ObjectAllowed(repo.Namespace, repo.Labels) {
		logger.V(1).Info("skipped by watch filter")
		return ctrl.Result{}, nil
	}
	if repo.Spec.URL == "" || repo.Spec.PasswordSecretRef.Name == "" {
		logger.Info("invalid repository spec", "reason", "url and passwordSecretRef required")
		repo.Status.Phase = "Failed"
		repo.Status.Conditions = []operatorv1alpha1.Condition{{Type: "Invalid", Status: "True", Message: "url and passwordSecretRef required"}}
		_ = r.Status().Update(ctx, &repo)
		return ctrl.Result{}, nil
	}
	verifyEnabled := true
	if repo.Spec.Verify.Enabled != nil {
		verifyEnabled = *repo.Spec.Verify.Enabled
	}
	repo.Status.Phase = "Ready"
	repo.Status.ResticURL = repo.Spec.URL
	if verifyEnabled {
		repo.Status.LastCheckResult = "scheduled"
		if err := r.ensureCheckCron(ctx, &repo); err != nil {
			metrics.ReconcileErrors.WithLabelValues("BackupRepository").Inc()
			logger.Error(err, "ensure restic check CronJob failed")
			return ctrl.Result{}, err
		}
		logger.V(1).Info("verify CronJob ensured", "url", repo.Spec.URL)
	} else {
		repo.Status.LastCheckResult = "skipped"
		name := "restic-check-" + repo.Name
		_ = client.IgnoreNotFound(r.Delete(ctx, &batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: repo.Namespace}}))
		logger.V(1).Info("verify disabled")
	}
	if err := syncRepositoryToHost(ctx, r.Client, &repo); err != nil {
		metrics.ReconcileErrors.WithLabelValues("BackupRepository").Inc()
		logger.Error(err, "failed to sync repository to Backrest UI")
		repo.Status.Phase = "Failed"
		repo.Status.Conditions = []operatorv1alpha1.Condition{{Type: "SyncFailed", Status: "True", Message: err.Error()}}
		_ = r.Status().Update(ctx, &repo)
		return ctrl.Result{}, err
	}
	logger.V(1).Info("repository ready", "url", repo.Spec.URL, "verify", verifyEnabled, "syncToHost", repo.Spec.Backrest.SyncToHost)
	return ctrl.Result{}, r.Status().Update(ctx, &repo)
}

func (r *BackupRepositoryReconciler) ensureCheckCron(ctx context.Context, repo *operatorv1alpha1.BackupRepository) error {
	schedule := repo.Spec.Verify.Schedule
	if schedule == "" {
		schedule = "0 3 * * 0"
	}
	name := "restic-check-" + repo.Name
	env := []corev1.EnvVar{
		{Name: "RESTIC_REPOSITORY", Value: repo.Spec.URL},
		{Name: "RESTIC_PASSWORD", ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: repo.Spec.PasswordSecretRef.Name},
				Key:                  keyOr(repo.Spec.PasswordSecretRef.Key, "RESTIC_PASSWORD"),
			},
		}},
	}
	container := corev1.Container{
		Name:    "restic",
		Image:   resticImage,
		Command: []string{"sh", "-ec", resticCheckJobScript()},
		Env:     env,
	}
	if repo.Spec.EnvFromSecretRef != nil && repo.Spec.EnvFromSecretRef.Name != "" {
		container.EnvFrom = []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: repo.Spec.EnvFromSecretRef.Name}}}}
	}
	enableLinks := false
	cron := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: repo.Namespace},
		Spec: batchv1.CronJobSpec{
			Schedule:          schedule,
			ConcurrencyPolicy: batchv1.ForbidConcurrent,
			JobTemplate: batchv1.JobTemplateSpec{
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							RestartPolicy:      corev1.RestartPolicyOnFailure,
							EnableServiceLinks: &enableLinks,
							Containers:         []corev1.Container{container},
						},
					},
				},
			},
		},
	}
	_ = controllerutil.SetControllerReference(repo, cron, r.Scheme)
	var cur batchv1.CronJob
	err := r.Get(ctx, client.ObjectKeyFromObject(cron), &cur)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, cron)
	}
	if err != nil {
		return err
	}
	cur.Spec = cron.Spec
	return r.Update(ctx, &cur)
}

func keyOr(k, def string) string {
	if k == "" {
		return def
	}
	return k
}

func (r *BackupRepositoryReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&operatorv1alpha1.BackupRepository{}).
		Owns(&batchv1.CronJob{}).
		Complete(r)
}

// --- BackupPlan ---

type BackupPlanReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

func (r *BackupPlanReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("backupplan", req.NamespacedName)
	var plan operatorv1alpha1.BackupPlan
	if err := r.Get(ctx, req.NamespacedName, &plan); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !filters.ObjectAllowed(plan.Namespace, plan.Labels) {
		logger.V(1).Info("skipped by watch filter")
		return ctrl.Result{}, nil
	}
	if plan.Spec.RepositoryRef.Name == "" {
		logger.Info("invalid plan spec", "reason", "repositoryRef.name required")
		plan.Status.Phase = "Failed"
		return ctrl.Result{}, r.Status().Update(ctx, &plan)
	}

	repoNS := plan.Spec.RepositoryRef.Namespace
	if repoNS == "" {
		repoNS = plan.Namespace
	}
	var repo operatorv1alpha1.BackupRepository
	if err := r.Get(ctx, client.ObjectKey{Namespace: repoNS, Name: plan.Spec.RepositoryRef.Name}, &repo); err != nil {
		logger.Error(err, "get repository for plan sync")
		plan.Status.Phase = "Failed"
		_ = r.Status().Update(ctx, &plan)
		return ctrl.Result{}, err
	}

	if repo.Spec.Backrest.SyncToHost || plan.Spec.ClusterRef.Name != "" {
		if err := syncPlanToHost(ctx, r.Client, &plan, &repo); err != nil {
			metrics.ReconcileErrors.WithLabelValues("BackupPlan").Inc()
			logger.Error(err, "sync plan to Backrest host failed")
			plan.Status.Phase = "Failed"
			_ = r.Status().Update(ctx, &plan)
			return ctrl.Result{}, err
		}
	} else {
		logger.Info("plan sync skipped (set BackupRepository.spec.backrest.syncToHost or plan.clusterRef)")
	}

	if err := r.propagateRetriesToPVCBackup(ctx, &plan); err != nil {
		logger.Error(err, "propagate retries to PVCBackup")
		return ctrl.Result{}, err
	}

	plan.Status.Phase = "Ready"
	return ctrl.Result{}, r.Status().Update(ctx, &plan)
}

func (r *BackupPlanReconciler) propagateRetriesToPVCBackup(ctx context.Context, plan *operatorv1alpha1.BackupPlan) error {
	if plan.Spec.PVCBackupRef.Name == "" || plan.Spec.Retries == nil {
		return nil
	}
	ns := plan.Spec.PVCBackupRef.Namespace
	if ns == "" {
		ns = plan.Namespace
	}
	var b operatorv1alpha1.PVCBackup
	if err := r.Get(ctx, client.ObjectKey{Namespace: ns, Name: plan.Spec.PVCBackupRef.Name}, &b); err != nil {
		return client.IgnoreNotFound(err)
	}
	if b.Spec.Retries != nil && *b.Spec.Retries == *plan.Spec.Retries {
		return nil
	}
	b.Spec.Retries = plan.Spec.Retries
	return r.Update(ctx, &b)
}

func orStrings(v []string) []string {
	if v == nil {
		return []string{}
	}
	return v
}

func (r *BackupPlanReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&operatorv1alpha1.BackupPlan{}).
		Complete(r)
}
