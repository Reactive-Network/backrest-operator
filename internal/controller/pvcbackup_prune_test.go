package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	operatorv1alpha1 "github.com/Reactive-Network/backrest-operator/api/v1alpha1"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := operatorv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestForcePrunePending(t *testing.T) {
	keep := int32(3)
	b := &operatorv1alpha1.PVCBackup{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{annForcePrune: "token-2"},
		},
		Spec: operatorv1alpha1.PVCBackupSpec{
			Retention: operatorv1alpha1.PVCBackupRetention{KeepLast: &keep},
		},
		Status: operatorv1alpha1.PVCBackupStatus{LastForcePrune: "token-1"},
	}
	if !forcePrunePending(b) {
		t.Fatal("expected forcePrunePending when annotation != lastForcePrune")
	}
	b.Status.LastForcePrune = "token-2"
	if forcePrunePending(b) {
		t.Fatal("expected not pending after token claim")
	}
}

func TestBuildResticPruneJobNoPVCMounts(t *testing.T) {
	keep := int32(5)
	b := &operatorv1alpha1.PVCBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "net-a"},
		Spec: operatorv1alpha1.PVCBackupSpec{
			PVCName: "vol",
			Retention: operatorv1alpha1.PVCBackupRetention{
				KeepLast: &keep,
			},
		},
	}
	repo := &operatorv1alpha1.BackupRepository{
		Spec: operatorv1alpha1.BackupRepositorySpec{
			URL: "s3:s3.amazonaws.com/bucket",
			PasswordSecretRef: operatorv1alpha1.SecretKeySelector{
				Name: "repo-pass",
			},
		},
	}
	job := buildResticPruneJob(b, repo, "pvcbackup-prune-data-1")
	if len(job.Spec.Template.Spec.Volumes) != 0 {
		t.Fatalf("expected no volumes, got %d", len(job.Spec.Template.Spec.Volumes))
	}
	if len(job.Spec.Template.Spec.Containers) != 1 {
		t.Fatalf("expected one container, got %d", len(job.Spec.Template.Spec.Containers))
	}
	c := job.Spec.Template.Spec.Containers[0]
	if len(c.VolumeMounts) != 0 {
		t.Fatalf("expected no volume mounts, got %d", len(c.VolumeMounts))
	}
	script := strings.Join(c.Command, " ")
	for _, want := range []string{"forget", "--prune", "--keep-last 5", "--group-by ''"} {
		if !strings.Contains(script, want) {
			t.Fatalf("script missing %q: %s", want, script)
		}
	}
	if !strings.Contains(script, shellQuoteOne(pruneJobScript(b))) && !strings.Contains(script, "forget") {
		t.Fatalf("unexpected script: %s", script)
	}
}

func TestPollPruneJobSuccessPreservesBackupTimestamps(t *testing.T) {
	scheme := testScheme(t)
	keep := int32(3)
	backup := &operatorv1alpha1.PVCBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data",
			Namespace: "net-a",
		},
		Spec: operatorv1alpha1.PVCBackupSpec{
			PVCName: "vol",
			RepositoryRef: operatorv1alpha1.ObjectReference{
				Name: "repo",
			},
			Retention: operatorv1alpha1.PVCBackupRetention{KeepLast: &keep},
		},
		Status: operatorv1alpha1.PVCBackupStatus{
			Phase:           "Pruning",
			LastJobName:     "pvcbackup-prune-data-1",
			LastBackupTime:  "2026-01-01T00:00:00Z",
			LastSuccessTime: "2026-01-01T00:00:00Z",
		},
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pvcbackup-prune-data-1",
			Namespace: "net-a",
		},
		Status: batchv1.JobStatus{Succeeded: 1},
	}
	repo := &operatorv1alpha1.BackupRepository{
		ObjectMeta: metav1.ObjectMeta{Name: "repo", Namespace: "net-a"},
		Spec: operatorv1alpha1.BackupRepositorySpec{
			URL: "s3:s3.amazonaws.com/bucket",
			PasswordSecretRef: operatorv1alpha1.SecretKeySelector{
				Name: "repo-pass",
			},
			Backrest: operatorv1alpha1.RepoBackrestSpec{
				ClusterRef: operatorv1alpha1.ObjectReference{Name: "main"},
			},
		},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "repo-pass", Namespace: "net-a"},
		Data:       map[string][]byte{"RESTIC_PASSWORD": []byte("pw")},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(backup, job, repo, secret).
		WithStatusSubresource(backup).
		Build()

	r := &PVCBackupReconciler{Client: c, Scheme: scheme}
	if _, err := r.pollPruneJob(context.Background(), backup); err != nil {
		t.Fatalf("pollPruneJob: %v", err)
	}
	var got operatorv1alpha1.PVCBackup
	if err := c.Get(context.Background(), types.NamespacedName{Name: "data", Namespace: "net-a"}, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != "Succeeded" {
		t.Fatalf("phase = %q, want Succeeded", got.Status.Phase)
	}
	if got.Status.LastBackupTime != "2026-01-01T00:00:00Z" {
		t.Fatalf("lastBackupTime changed to %q", got.Status.LastBackupTime)
	}
	if got.Status.LastSuccessTime != "2026-01-01T00:00:00Z" {
		t.Fatalf("lastSuccessTime changed to %q", got.Status.LastSuccessTime)
	}
	if got.Annotations != nil && got.Annotations[annQuiesceState] != "" {
		t.Fatal("unexpected quiesce annotation after prune success")
	}
}

func TestReconcilePruningBlocksForceRun(t *testing.T) {
	scheme := testScheme(t)
	keep := int32(3)
	backup := &operatorv1alpha1.PVCBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "data",
			Namespace:  "net-a",
			Finalizers: []string{finalizerHostPlan},
			Annotations: map[string]string{
				annForceRun: "run-token",
			},
		},
		Spec: operatorv1alpha1.PVCBackupSpec{
			PVCName: "vol",
			RepositoryRef: operatorv1alpha1.ObjectReference{
				Name: "repo",
			},
			Quiesce: operatorv1alpha1.QuiesceSpec{
				Enabled: true,
				Targets: []operatorv1alpha1.QuiesceTarget{
					{Kind: "StatefulSet", Name: "node", Namespace: "net-a"},
				},
			},
			Retention: operatorv1alpha1.PVCBackupRetention{KeepLast: &keep},
		},
		Status: operatorv1alpha1.PVCBackupStatus{
			Phase:       "Pruning",
			LastJobName: "pvcbackup-prune-data-1",
		},
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pvcbackup-prune-data-1",
			Namespace: "net-a",
		},
	}
	repo := &operatorv1alpha1.BackupRepository{
		ObjectMeta: metav1.ObjectMeta{Name: "repo", Namespace: "net-a"},
		Spec: operatorv1alpha1.BackupRepositorySpec{
			URL: "s3:s3.amazonaws.com/bucket",
			PasswordSecretRef: operatorv1alpha1.SecretKeySelector{
				Name: "repo-pass",
			},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(backup, job, repo).
		WithStatusSubresource(backup, job).
		Build()

	r := &PVCBackupReconciler{Client: c, Scheme: scheme}
	res, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Name: "data", Namespace: "net-a"}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter != 15*time.Second {
		t.Fatalf("expected requeue poll, got RequeueAfter=%s", res.RequeueAfter)
	}
	var got operatorv1alpha1.PVCBackup
	if err := c.Get(context.Background(), types.NamespacedName{Name: "data", Namespace: "net-a"}, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != "Pruning" {
		t.Fatalf("phase = %q, want Pruning (backup must not start)", got.Status.Phase)
	}
	if got.Status.LastForceRun != "" {
		t.Fatalf("force-run token claimed during prune: lastForceRun=%q", got.Status.LastForceRun)
	}
	if got.Annotations[annQuiesceState] != "" {
		t.Fatal("quiesce annotation set during prune reconcile")
	}
}

func TestForcePruneClaimsTokenBeforeJob(t *testing.T) {
	scheme := testScheme(t)
	keep := int32(2)
	backup := &operatorv1alpha1.PVCBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "data",
			Namespace:  "net-a",
			Finalizers: []string{finalizerHostPlan},
			Annotations: map[string]string{
				annForcePrune: "prune-token",
			},
		},
		Spec: operatorv1alpha1.PVCBackupSpec{
			PVCName: "vol",
			RepositoryRef: operatorv1alpha1.ObjectReference{
				Name: "repo",
			},
			Retention: operatorv1alpha1.PVCBackupRetention{KeepLast: &keep},
		},
		Status: operatorv1alpha1.PVCBackupStatus{
			Phase: "Succeeded",
		},
	}
	repo := &operatorv1alpha1.BackupRepository{
		ObjectMeta: metav1.ObjectMeta{Name: "repo", Namespace: "net-a"},
		Spec: operatorv1alpha1.BackupRepositorySpec{
			URL: "s3:s3.amazonaws.com/bucket",
			PasswordSecretRef: operatorv1alpha1.SecretKeySelector{
				Name: "repo-pass",
			},
		},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "repo-pass", Namespace: "net-a"},
		Data:       map[string][]byte{"RESTIC_PASSWORD": []byte("pw")},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(backup, repo, secret).
		WithStatusSubresource(backup).
		Build()

	r := &PVCBackupReconciler{Client: c, Scheme: scheme}
	_, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Name: "data", Namespace: "net-a"}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	var got operatorv1alpha1.PVCBackup
	if err := c.Get(context.Background(), types.NamespacedName{Name: "data", Namespace: "net-a"}, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.LastForcePrune != "prune-token" {
		t.Fatalf("lastForcePrune = %q, want prune-token", got.Status.LastForcePrune)
	}
	if got.Status.Phase != "Pruning" {
		t.Fatalf("phase = %q, want Pruning", got.Status.Phase)
	}
	var jobs batchv1.JobList
	if err := c.List(context.Background(), &jobs, client.InNamespace("net-a")); err != nil {
		t.Fatal(err)
	}
	if len(jobs.Items) != 1 {
		t.Fatalf("expected one prune job, got %d", len(jobs.Items))
	}
	job := jobs.Items[0]
	if !strings.HasPrefix(job.Name, "pvcbackup-prune-data-") {
		t.Fatalf("unexpected job name %q", job.Name)
	}
	if len(job.Spec.Template.Spec.Volumes) != 0 || len(job.Spec.Template.Spec.Containers[0].VolumeMounts) != 0 {
		t.Fatal("prune job must not mount PVC volumes")
	}
}

func TestReconcileUploadingDefersForcePrune(t *testing.T) {
	scheme := testScheme(t)
	keep := int32(2)
	backup := &operatorv1alpha1.PVCBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "data",
			Namespace:  "net-a",
			Finalizers: []string{finalizerHostPlan},
			Annotations: map[string]string{
				annForcePrune: "prune-token",
			},
		},
		Spec: operatorv1alpha1.PVCBackupSpec{
			PVCName: "vol",
			RepositoryRef: operatorv1alpha1.ObjectReference{
				Name: "repo",
			},
			Retention: operatorv1alpha1.PVCBackupRetention{KeepLast: &keep},
		},
		Status: operatorv1alpha1.PVCBackupStatus{
			Phase:       "Uploading",
			LastJobName: "pvcbackup-data-1",
		},
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pvcbackup-data-1",
			Namespace: "net-a",
		},
	}
	repo := &operatorv1alpha1.BackupRepository{
		ObjectMeta: metav1.ObjectMeta{Name: "repo", Namespace: "net-a"},
		Spec: operatorv1alpha1.BackupRepositorySpec{
			URL: "s3:s3.amazonaws.com/bucket",
			PasswordSecretRef: operatorv1alpha1.SecretKeySelector{
				Name: "repo-pass",
			},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(backup, job, repo).
		WithStatusSubresource(backup, job).
		Build()

	r := &PVCBackupReconciler{Client: c, Scheme: scheme}
	res, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Name: "data", Namespace: "net-a"}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter != 15*time.Second {
		t.Fatalf("expected backup poll requeue, got RequeueAfter=%s", res.RequeueAfter)
	}
	var got operatorv1alpha1.PVCBackup
	if err := c.Get(context.Background(), types.NamespacedName{Name: "data", Namespace: "net-a"}, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != "Uploading" {
		t.Fatalf("phase = %q, want Uploading", got.Status.Phase)
	}
	if got.Status.LastForcePrune != "" {
		t.Fatalf("lastForcePrune claimed during backup upload: %q", got.Status.LastForcePrune)
	}
	var jobs batchv1.JobList
	if err := c.List(context.Background(), &jobs, client.InNamespace("net-a")); err != nil {
		t.Fatal(err)
	}
	if len(jobs.Items) != 1 || jobs.Items[0].Name != "pvcbackup-data-1" {
		t.Fatalf("expected only backup job, got %d jobs", len(jobs.Items))
	}
}

func TestReconcileQuiescingDefersForcePrune(t *testing.T) {
	scheme := testScheme(t)
	keep := int32(2)
	backup := &operatorv1alpha1.PVCBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "data",
			Namespace:  "net-a",
			Finalizers: []string{finalizerHostPlan},
			Annotations: map[string]string{
				annForcePrune: "prune-token",
			},
		},
		Spec: operatorv1alpha1.PVCBackupSpec{
			PVCName: "vol",
			RepositoryRef: operatorv1alpha1.ObjectReference{
				Name: "repo",
			},
			Retention: operatorv1alpha1.PVCBackupRetention{KeepLast: &keep},
		},
		Status: operatorv1alpha1.PVCBackupStatus{
			Phase: "Quiescing",
		},
	}
	repo := &operatorv1alpha1.BackupRepository{
		ObjectMeta: metav1.ObjectMeta{Name: "repo", Namespace: "net-a"},
		Spec: operatorv1alpha1.BackupRepositorySpec{
			URL: "s3:s3.amazonaws.com/bucket",
			PasswordSecretRef: operatorv1alpha1.SecretKeySelector{
				Name: "repo-pass",
			},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(backup, repo).
		WithStatusSubresource(backup).
		Build()

	r := &PVCBackupReconciler{Client: c, Scheme: scheme}
	res, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Name: "data", Namespace: "net-a"}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter != 30*time.Second {
		t.Fatalf("expected defer requeue, got RequeueAfter=%s", res.RequeueAfter)
	}
	var got operatorv1alpha1.PVCBackup
	if err := c.Get(context.Background(), types.NamespacedName{Name: "data", Namespace: "net-a"}, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != "Quiescing" {
		t.Fatalf("phase = %q, want Quiescing", got.Status.Phase)
	}
	if got.Status.LastForcePrune != "" {
		t.Fatalf("lastForcePrune claimed while backup in-flight: %q", got.Status.LastForcePrune)
	}
	var jobs batchv1.JobList
	if err := c.List(context.Background(), &jobs, client.InNamespace("net-a")); err != nil {
		t.Fatal(err)
	}
	if len(jobs.Items) != 0 {
		t.Fatalf("expected no prune job, got %d", len(jobs.Items))
	}
}
