package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	operatorv1alpha1 "github.com/Reactive-Network/backrest-operator/api/v1alpha1"
	"github.com/Reactive-Network/backrest-operator/internal/backrest"
)

// fakeBackrest mimics a freshly started Backrest host: default config with auth disabled,
// RequireAuthentication on the API, and the RPCs the operator uses for repositories.
type fakeBackrest struct {
	mu  sync.Mutex
	cfg backrest.Config
}

func newFakeBackrest() *fakeBackrest {
	return &fakeBackrest{cfg: backrest.Config{Version: 6, Auth: &backrest.Auth{Disabled: true}}}
}

func (f *fakeBackrest) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.cfg.Auth != nil && !f.cfg.Auth.Disabled {
		u, p, ok := r.BasicAuth()
		allowed := false
		for _, usr := range f.cfg.Auth.Users {
			if ok && usr.Name == u && backrest.PasswordMatches(usr.PasswordBcrypt, p) {
				allowed = true
			}
		}
		if !allowed {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
	}
	switch r.URL.Path {
	case "/v1.Backrest/GetConfig":
		_ = json.NewEncoder(w).Encode(f.cfg)
	case "/v1.Backrest/SetConfig":
		var in backrest.Config
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.Modno != f.cfg.Modno {
			http.Error(w, "bad config or modno mismatch", http.StatusBadRequest)
			return
		}
		in.Modno++
		f.cfg = in
		_ = json.NewEncoder(w).Encode(f.cfg)
	case "/v1.Backrest/AddRepo":
		var in struct {
			Repo backrest.Repo `json:"repo"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		f.cfg.Repos = append(f.cfg.Repos, in.Repo)
		f.cfg.Modno++
		_ = json.NewEncoder(w).Encode(f.cfg)
	case "/v1.Backrest/DoRepoTask":
		_, _ = w.Write([]byte("{}"))
	default:
		http.NotFound(w, r)
	}
}

func (f *fakeBackrest) snapshot() backrest.Config {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cfg
}

func withFakeHost(t *testing.T, h http.Handler) {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	prev := hostURLFor
	hostURLFor = func(string, string) string { return srv.URL }
	t.Cleanup(func() { hostURLFor = prev })
	t.Setenv(backrest.EnvAuthUsername, "")
	t.Setenv(backrest.EnvAuthPassword, "")
}

func authCluster(enabled bool, secret string) *operatorv1alpha1.BackrestCluster {
	return &operatorv1alpha1.BackrestCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "main", Namespace: "backrest"},
		Spec: operatorv1alpha1.BackrestClusterSpec{
			Auth: operatorv1alpha1.BackrestAuthSpec{Enabled: enabled, ExistingSecret: secret},
		},
	}
}

func authSecret(user, pass string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "backrest-api-auth", Namespace: "backrest"},
		Data: map[string][]byte{
			backrest.SecretKeyUsername: []byte(user),
			backrest.SecretKeyPassword: []byte(pass),
		},
	}
}

func syncedRepo(name, ns string) (*operatorv1alpha1.BackupRepository, *corev1.Secret) {
	repo := &operatorv1alpha1.BackupRepository{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: operatorv1alpha1.BackupRepositorySpec{
			URL:               "s3:https://example/" + name,
			PasswordSecretRef: operatorv1alpha1.SecretKeySelector{Name: name + "-pw", Key: "RESTIC_PASSWORD"},
			Backrest: operatorv1alpha1.RepoBackrestSpec{
				SyncToHost: true,
				ClusterRef: operatorv1alpha1.ObjectReference{Name: "main", Namespace: "backrest"},
			},
		},
	}
	pw := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name + "-pw", Namespace: ns},
		Data:       map[string][]byte{"RESTIC_PASSWORD": []byte("restic")},
	}
	return repo, pw
}

func TestHostAuthForCluster(t *testing.T) {
	ctx := context.Background()
	s := testScheme(t)

	// Disabled, no secret: disable policy, no credentials (env fallback).
	c := fake.NewClientBuilder().WithScheme(s).Build()
	p, cr, err := hostAuthForCluster(ctx, c, authCluster(false, ""))
	if err != nil || p == nil || p.Enabled || !cr.Empty() {
		t.Fatalf("disabled: p=%+v cr=%+v err=%v", p, cr, err)
	}
	// Disabled, secret named but missing: still fine.
	if _, _, err := hostAuthForCluster(ctx, c, authCluster(false, "backrest-api-auth")); err != nil {
		t.Fatalf("disabled with missing secret: %v", err)
	}
	// Enabled without a secret name.
	if _, _, err := hostAuthForCluster(ctx, c, authCluster(true, "")); err == nil {
		t.Fatal("enabled without existingSecret must fail")
	}
	// Enabled, secret missing.
	if _, _, err := hostAuthForCluster(ctx, c, authCluster(true, "backrest-api-auth")); err == nil {
		t.Fatal("enabled with missing secret must fail")
	}
	// Enabled, secret without password.
	c = fake.NewClientBuilder().WithScheme(s).WithObjects(authSecret("operator", "")).Build()
	if _, _, err := hostAuthForCluster(ctx, c, authCluster(true, "backrest-api-auth")); err == nil || !strings.Contains(err.Error(), "password") {
		t.Fatalf("secret without password: err=%v", err)
	}
	// Enabled, complete secret.
	c = fake.NewClientBuilder().WithScheme(s).WithObjects(authSecret("operator\n", "pw1")).Build()
	p, cr, err = hostAuthForCluster(ctx, c, authCluster(true, "backrest-api-auth"))
	if err != nil || !p.Enabled || len(p.Users) != 1 || cr.Username != "operator" || cr.Password != "pw1" {
		t.Fatalf("enabled: p=%+v cr=%+v err=%v", p, cr, err)
	}
	// Disabled with a complete secret keeps the credentials, so auth can be switched off.
	p, cr, err = hostAuthForCluster(ctx, c, authCluster(false, "backrest-api-auth"))
	if err != nil || p.Enabled || cr.Empty() {
		t.Fatalf("disabled with secret: p=%+v cr=%+v err=%v", p, cr, err)
	}
}

// A restarted host has the default config (auth off, no repos). One cluster reconcile must
// turn auth on, re-add the repository, and leave a host the operator can still talk to.
func TestSyncClusterHostRestoresAuthAndRepos(t *testing.T) {
	host := newFakeBackrest()
	withFakeHost(t, host)
	ctx := context.Background()
	cluster := authCluster(true, "backrest-api-auth")
	repo, repoPW := syncedRepo("chain-repo", "chain")
	otherRepo, otherPW := syncedRepo("not-synced", "chain")
	otherRepo.Spec.Backrest.SyncToHost = false
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).
		WithObjects(cluster, authSecret("operator", "pw1"), repo, repoPW, otherRepo, otherPW).Build()

	if err := syncClusterHost(ctx, c, cluster); err != nil {
		t.Fatal(err)
	}
	cfg := host.snapshot()
	if cfg.Instance != "main" {
		t.Fatalf("instance=%q", cfg.Instance)
	}
	if cfg.Auth == nil || cfg.Auth.Disabled || len(cfg.Auth.Users) != 1 || cfg.Auth.Users[0].Name != "operator" {
		t.Fatalf("auth=%+v", cfg.Auth)
	}
	if !backrest.PasswordMatches(cfg.Auth.Users[0].PasswordBcrypt, "pw1") {
		t.Fatal("stored hash does not match the secret")
	}
	if len(cfg.Repos) != 1 || cfg.Repos[0].ID != "chain-repo" {
		t.Fatalf("repos=%+v", cfg.Repos)
	}

	// Second pass: nothing to do, and every call carries working credentials.
	modno := cfg.Modno
	if err := syncClusterHost(ctx, c, cluster); err != nil {
		t.Fatalf("second sync: %v", err)
	}
	if got := host.snapshot().Modno; got != modno {
		t.Fatalf("second sync rewrote the config: modno %d -> %d", modno, got)
	}

	// Plan sync after a backup authenticates too.
	b := &operatorv1alpha1.PVCBackup{ObjectMeta: metav1.ObjectMeta{Name: "node-0", Namespace: "chain"}}
	b.Spec.PVCName = "node-0-data"
	if err := syncPVCBackupPlanToHost(ctx, c, b, repo); err != nil {
		t.Fatalf("plan sync with auth on: %v", err)
	}
	if got := host.snapshot().Plans; len(got) != 1 || got[0].Repo != "chain-repo" {
		t.Fatalf("plans=%+v", got)
	}
}

// Switching spec.auth.enabled back to false must work through the authenticated client.
func TestSyncClusterHostDisablesAuth(t *testing.T) {
	host := newFakeBackrest()
	withFakeHost(t, host)
	ctx := context.Background()
	on := authCluster(true, "backrest-api-auth")
	sec := authSecret("operator", "pw1")
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(on, sec).Build()
	if err := syncClusterHost(ctx, c, on); err != nil {
		t.Fatal(err)
	}
	off := authCluster(false, "backrest-api-auth")
	c = fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(off, sec).Build()
	if err := syncClusterHost(ctx, c, off); err != nil {
		t.Fatalf("disable: %v", err)
	}
	if cfg := host.snapshot(); cfg.Auth == nil || !cfg.Auth.Disabled || len(cfg.Auth.Users) != 0 {
		t.Fatalf("auth=%+v", cfg.Auth)
	}
}

// A wrong password in the Secret must surface as an error, not as a silent success.
func TestSyncClusterHostWrongPassword(t *testing.T) {
	host := newFakeBackrest()
	withFakeHost(t, host)
	ctx := context.Background()
	cluster := authCluster(true, "backrest-api-auth")
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(cluster, authSecret("operator", "pw1")).Build()
	if err := syncClusterHost(ctx, c, cluster); err != nil {
		t.Fatal(err)
	}
	c = fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(cluster, authSecret("operator", "changed-elsewhere")).Build()
	err := syncClusterHost(ctx, c, cluster)
	if err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("expected 401, got %v", err)
	}
}

func TestKeepRestartedAt(t *testing.T) {
	cur := &corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{annRestartedAt: "2026-11-23T20:43:33+02:00"}}}
	desired := &corev1.PodTemplateSpec{}
	keepRestartedAt(cur, desired)
	if desired.Annotations[annRestartedAt] != "2026-11-23T20:43:33+02:00" {
		t.Fatalf("annotation not kept: %+v", desired.Annotations)
	}
	none := &corev1.PodTemplateSpec{}
	desired = &corev1.PodTemplateSpec{}
	keepRestartedAt(none, desired)
	if desired.Annotations != nil {
		t.Fatalf("unexpected annotations: %+v", desired.Annotations)
	}
}
