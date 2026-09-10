package controller

import (
	"context"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	operatorv1alpha1 "github.com/Reactive-Network/backrest-operator/api/v1alpha1"
)

func TestForceCancelPending(t *testing.T) {
	b := &operatorv1alpha1.PVCBackup{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{annForceCancel: "token-2"},
		},
		Status: operatorv1alpha1.PVCBackupStatus{LastForceCancel: "token-1"},
	}
	if !forceCancelPending(b) {
		t.Fatal("expected forceCancelPending when annotation != lastForceCancel")
	}
	b.Status.LastForceCancel = "token-2"
	if forceCancelPending(b) {
		t.Fatal("expected not pending after token claim")
	}
}

func TestForceCancelDeletesUploadingJob(t *testing.T) {
	scheme := testScheme(t)
	backup := &operatorv1alpha1.PVCBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "data",
			Namespace:  "net-a",
			Finalizers: []string{finalizerHostPlan},
			Annotations: map[string]string{
				annForceCancel: "cancel-token",
			},
		},
		Spec: operatorv1alpha1.PVCBackupSpec{
			PVCName: "vol",
			RepositoryRef: operatorv1alpha1.ObjectReference{
				Name: "repo",
			},
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
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(backup, job).
		WithStatusSubresource(backup, job).
		Build()

	r := &PVCBackupReconciler{Client: c, Scheme: scheme}
	_, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "data", Namespace: "net-a"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	var got operatorv1alpha1.PVCBackup
	if err := c.Get(context.Background(), types.NamespacedName{Name: "data", Namespace: "net-a"}, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != "Failed" {
		t.Fatalf("phase = %q, want Failed", got.Status.Phase)
	}
	if got.Status.LastForceCancel != "cancel-token" {
		t.Fatalf("lastForceCancel = %q", got.Status.LastForceCancel)
	}
	if len(got.Status.Conditions) == 0 || got.Status.Conditions[0].Message != "cancelled by user" {
		t.Fatalf("conditions = %+v", got.Status.Conditions)
	}
	err = c.Get(context.Background(), types.NamespacedName{Name: "pvcbackup-data-1", Namespace: "net-a"}, &batchv1.Job{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected job NotFound, got %v", err)
	}
}

func TestForceCancelIdleClaimsTokenOnly(t *testing.T) {
	scheme := testScheme(t)
	backup := &operatorv1alpha1.PVCBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "data",
			Namespace:  "net-a",
			Finalizers: []string{finalizerHostPlan},
			Annotations: map[string]string{
				annForceCancel: "cancel-idle",
			},
		},
		Spec: operatorv1alpha1.PVCBackupSpec{
			PVCName: "vol",
			RepositoryRef: operatorv1alpha1.ObjectReference{
				Name: "repo",
			},
		},
		Status: operatorv1alpha1.PVCBackupStatus{
			Phase: "Succeeded",
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(backup).
		WithStatusSubresource(backup).
		Build()

	r := &PVCBackupReconciler{Client: c, Scheme: scheme}
	_, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "data", Namespace: "net-a"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	var got operatorv1alpha1.PVCBackup
	if err := c.Get(context.Background(), types.NamespacedName{Name: "data", Namespace: "net-a"}, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != "Succeeded" {
		t.Fatalf("phase = %q, want Succeeded (idle cancel)", got.Status.Phase)
	}
	if got.Status.LastForceCancel != "cancel-idle" {
		t.Fatalf("lastForceCancel = %q", got.Status.LastForceCancel)
	}
}
