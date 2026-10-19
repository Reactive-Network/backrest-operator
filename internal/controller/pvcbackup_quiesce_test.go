package controller

import (
	"context"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	operatorv1alpha1 "github.com/Reactive-Network/backrest-operator/api/v1alpha1"
)

func quiescedBackup(phase, jobName, quiesceState string, target operatorv1alpha1.QuiesceTarget) *operatorv1alpha1.PVCBackup {
	b := &operatorv1alpha1.PVCBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "data",
			Namespace:  "net-a",
			Finalizers: []string{finalizerHostPlan},
		},
		Spec: operatorv1alpha1.PVCBackupSpec{
			PVCName:       "vol",
			RepositoryRef: operatorv1alpha1.ObjectReference{Name: "repo"},
			Strategy:      operatorv1alpha1.PVCBackupStrategy{Pipeline: []string{"quiescedLive"}},
			Quiesce: operatorv1alpha1.QuiesceSpec{
				Enabled: true,
				Targets: []operatorv1alpha1.QuiesceTarget{target},
			},
		},
		Status: operatorv1alpha1.PVCBackupStatus{Phase: phase, LastJobName: jobName},
	}
	if quiesceState != "" {
		b.Annotations = map[string]string{annQuiesceState: quiesceState}
	}
	return b
}

func testRepo() (*operatorv1alpha1.BackupRepository, *corev1.Secret) {
	repo := &operatorv1alpha1.BackupRepository{
		ObjectMeta: metav1.ObjectMeta{Name: "repo", Namespace: "net-a"},
		Spec: operatorv1alpha1.BackupRepositorySpec{
			URL:               "s3:s3.amazonaws.com/bucket",
			PasswordSecretRef: operatorv1alpha1.SecretKeySelector{Name: "repo-pass"},
			Backrest: operatorv1alpha1.RepoBackrestSpec{
				ClusterRef: operatorv1alpha1.ObjectReference{Name: "main"},
			},
		},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "repo-pass", Namespace: "net-a"},
		Data:       map[string][]byte{"RESTIC_PASSWORD": []byte("pw")},
	}
	return repo, secret
}

func reconcileData(t *testing.T, c client.Client, r *PVCBackupReconciler) operatorv1alpha1.PVCBackup {
	t.Helper()
	if _, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "data", Namespace: "net-a"},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	var got operatorv1alpha1.PVCBackup
	if err := c.Get(context.Background(), types.NamespacedName{Name: "data", Namespace: "net-a"}, &got); err != nil {
		t.Fatal(err)
	}
	return got
}

// The quiesce-state annotation is written between the Uploading status and the
// status write that points at the Job; that write must not hit a stale resourceVersion.
func TestReconcileQuiescedRunPointsStatusAtJob(t *testing.T) {
	scheme := testScheme(t)
	backup := quiescedBackup("", "", "", operatorv1alpha1.QuiesceTarget{Kind: "Pod", Name: "node"})
	backup.Annotations = map[string]string{annForceRun: "run-1"}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "node", Namespace: "net-a"}}
	repo, secret := testRepo()
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(backup, pod, repo, secret).
		WithStatusSubresource(backup).
		Build()

	got := reconcileData(t, c, &PVCBackupReconciler{Client: c, Scheme: scheme})
	if got.Status.Phase != "Uploading" {
		t.Fatalf("phase = %q, want Uploading", got.Status.Phase)
	}
	if !strings.HasPrefix(got.Status.LastJobName, "pvcbackup-data-") {
		t.Fatalf("lastJobName = %q, want the new Job", got.Status.LastJobName)
	}
	if got.Annotations[annQuiesceState] == "" {
		t.Fatal("quiesce state not recorded")
	}
	if err := c.Get(context.Background(), types.NamespacedName{Name: got.Status.LastJobName, Namespace: "net-a"}, &batchv1.Job{}); err != nil {
		t.Fatalf("job: %v", err)
	}
}

// Clearing the quiesce-state annotation precedes the success status write.
func TestPollBackupJobSuccessClearsQuiesceState(t *testing.T) {
	scheme := testScheme(t)
	backup := quiescedBackup("Uploading", "pvcbackup-data-1", `{"Pod/net-a/node":1}`,
		operatorv1alpha1.QuiesceTarget{Kind: "Pod", Name: "node"})
	backup.Spec.Schedule = "0 */4 * * *"
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "pvcbackup-data-1", Namespace: "net-a"},
		Status:     batchv1.JobStatus{Succeeded: 1},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(backup, job).
		WithStatusSubresource(backup).
		Build()

	got := reconcileData(t, c, &PVCBackupReconciler{Client: c, Scheme: scheme})
	if got.Status.Phase != "Scheduled" {
		t.Fatalf("phase = %q, want Scheduled", got.Status.Phase)
	}
	if got.Status.LastSuccessTime == "" {
		t.Fatal("lastSuccessTime not set")
	}
	if got.Annotations[annQuiesceState] != "" {
		t.Fatalf("quiesce state left behind: %q", got.Annotations[annQuiesceState])
	}
}

// A failed Job must record Failed and bring workloads back to the recorded
// replica count, not to the 1-replica fallback used when nothing was recorded.
func TestPollBackupJobFailureRestoresRecordedReplicas(t *testing.T) {
	scheme := testScheme(t)
	backup := quiescedBackup("Uploading", "pvcbackup-data-1", `{"StatefulSet/net-a/node":3}`,
		operatorv1alpha1.QuiesceTarget{Kind: "StatefulSet", Name: "node"})
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "pvcbackup-data-1", Namespace: "net-a"},
		Status:     batchv1.JobStatus{Failed: 1},
	}
	zero := int32(0)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "node", Namespace: "net-a"},
		Spec:       appsv1.StatefulSetSpec{Replicas: &zero},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(backup, job, sts).
		WithStatusSubresource(backup).
		Build()

	got := reconcileData(t, c, &PVCBackupReconciler{Client: c, Scheme: scheme})
	if got.Status.Phase != "Failed" {
		t.Fatalf("phase = %q, want Failed", got.Status.Phase)
	}
	if got.Annotations[annQuiesceState] != "" {
		t.Fatalf("quiesce state left behind: %q", got.Annotations[annQuiesceState])
	}
	var gotSts appsv1.StatefulSet
	if err := c.Get(context.Background(), types.NamespacedName{Name: "node", Namespace: "net-a"}, &gotSts); err != nil {
		t.Fatal(err)
	}
	if gotSts.Spec.Replicas == nil || *gotSts.Spec.Replicas != 3 {
		t.Fatalf("replicas = %v, want 3", gotSts.Spec.Replicas)
	}
}
