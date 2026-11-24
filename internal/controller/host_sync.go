package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	operatorv1alpha1 "github.com/Reactive-Network/backrest-operator/api/v1alpha1"
	"github.com/Reactive-Network/backrest-operator/internal/backrest"
	"github.com/Reactive-Network/backrest-operator/internal/filters"
)

// hostURLFor is swapped in tests to point at a fake Backrest.
var hostURLFor = backrest.HostURL

// hostClientForCluster returns a client for the BackrestCluster's host and the auth policy the
// operator enforces there. The client authenticates with the Secret named by
// spec.auth.existingSecret (keys username, password) when set, else with BACKREST_AUTH_* env.
// The policy is nil when the BackrestCluster does not exist; auth on such a host is left alone.
func hostClientForCluster(ctx context.Context, c client.Client, clusterNS, clusterName string) (*backrest.Client, *backrest.AuthPolicy, error) {
	bc := backrest.NewClient(hostURLFor(clusterNS, clusterName))
	var cluster operatorv1alpha1.BackrestCluster
	if err := c.Get(ctx, types.NamespacedName{Namespace: clusterNS, Name: clusterName}, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return bc, nil, nil
		}
		return nil, nil, err
	}
	policy, creds, err := hostAuthForCluster(ctx, c, &cluster)
	if err != nil {
		return nil, nil, err
	}
	bc.WithCredentials(creds)
	return bc, policy, nil
}

// hostAuthForCluster reads spec.auth. With auth disabled the policy writes {disabled: true};
// the Secret is still read when named, so the client keeps working while auth is switched off.
func hostAuthForCluster(ctx context.Context, c client.Client, cluster *operatorv1alpha1.BackrestCluster) (*backrest.AuthPolicy, backrest.Credentials, error) {
	spec := cluster.Spec.Auth
	var creds backrest.Credentials
	if spec.ExistingSecret != "" {
		var sec corev1.Secret
		err := c.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: spec.ExistingSecret}, &sec)
		switch {
		case err == nil:
			creds = backrest.Credentials{
				Username: strings.TrimSpace(string(sec.Data[backrest.SecretKeyUsername])),
				Password: string(sec.Data[backrest.SecretKeyPassword]),
			}
		case apierrors.IsNotFound(err) && !spec.Enabled:
			// Optional while auth is off.
		default:
			return nil, creds, fmt.Errorf("backrest auth secret %s/%s: %w", cluster.Namespace, spec.ExistingSecret, err)
		}
	}
	if !spec.Enabled {
		return &backrest.AuthPolicy{Enabled: false}, creds, nil
	}
	if spec.ExistingSecret == "" {
		return nil, creds, fmt.Errorf("BackrestCluster %s/%s: spec.auth.enabled requires spec.auth.existingSecret", cluster.Namespace, cluster.Name)
	}
	if creds.Empty() {
		return nil, creds, fmt.Errorf("backrest auth secret %s/%s: keys %q and %q must both be set", cluster.Namespace, spec.ExistingSecret, backrest.SecretKeyUsername, backrest.SecretKeyPassword)
	}
	return &backrest.AuthPolicy{Enabled: true, Users: []backrest.Credentials{creds}}, creds, nil
}

// connectHost returns a client for the host after making sure its instance ID and auth block
// match the BackrestCluster. Every sync goes through here, so auth is rewritten on each one.
func connectHost(ctx context.Context, c client.Client, clusterNS, clusterName string) (*backrest.Client, error) {
	bc, policy, err := hostClientForCluster(ctx, c, clusterNS, clusterName)
	if err != nil {
		return nil, err
	}
	if err := bc.EnsureHostConfig(ctx, instanceForCluster(clusterName), policy); err != nil {
		return nil, fmt.Errorf("ensure backrest instance: %w", err)
	}
	return bc, nil
}

// syncClusterHost brings a running host back in line with the cluster. Backrest keeps its
// config outside the PVC, so a restarted host comes up with the default config: auth disabled
// and no repositories. This restores instance and auth, then re-adds missing repositories.
func syncClusterHost(ctx context.Context, c client.Client, cluster *operatorv1alpha1.BackrestCluster) error {
	bc, err := connectHost(ctx, c, cluster.Namespace, cluster.Name)
	if err != nil {
		return err
	}
	cfg, err := bc.GetConfig(ctx)
	if err != nil {
		return err
	}
	present := make(map[string]struct{}, len(cfg.Repos))
	for _, r := range cfg.Repos {
		present[r.ID] = struct{}{}
	}
	var repos operatorv1alpha1.BackupRepositoryList
	if err := c.List(ctx, &repos); err != nil {
		return err
	}
	var errs []error
	for i := range repos.Items {
		repo := &repos.Items[i]
		if !repo.Spec.Backrest.SyncToHost || !filters.ObjectAllowed(repo.Namespace, repo.Labels) {
			continue
		}
		ns, name := resolveClusterRef(repo.Spec.Backrest.ClusterRef, repo.Namespace)
		if ns != cluster.Namespace || name != cluster.Name {
			continue
		}
		if _, ok := present[repo.Name]; ok {
			continue
		}
		log.FromContext(ctx).Info("repository missing on Backrest host, re-adding", "repo", repo.Name, "namespace", repo.Namespace)
		if err := syncRepositoryToHost(ctx, c, repo); err != nil {
			errs = append(errs, fmt.Errorf("repo %s/%s: %w", repo.Namespace, repo.Name, err))
		}
	}
	return errors.Join(errs...)
}

func resolveClusterRef(ref operatorv1alpha1.ObjectReference, defaultNS string) (ns, name string) {
	ns = ref.Namespace
	if ns == "" {
		ns = defaultNS
	}
	name = ref.Name
	if name == "" {
		name = "main"
	}
	return ns, name
}

func instanceForCluster(name string) string {
	// Backrest requires a non-empty instance; use the BackrestCluster name.
	return name
}

func planIDForPVCBackup(b *operatorv1alpha1.PVCBackup) string {
	return sanitizeID(b.Namespace + "-" + b.Name)
}

func sanitizeID(s string) string {
	s = strings.ToLower(s)
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteByte('-')
		}
	}
	out := b.String()
	if len(out) > 64 {
		out = out[:64]
	}
	return out
}

func pvcBackupPaths(b *operatorv1alpha1.PVCBackup) []string {
	pvcs := pvcList(b)
	paths := b.Spec.Paths
	if len(paths) == 1 && (paths[0] == "/" || paths[0] == "") {
		paths = nil
	}
	if len(paths) == 0 {
		out := make([]string, 0, len(pvcs))
		for _, pvc := range pvcs {
			out = append(out, "/data/"+sanitizePath(pvc))
		}
		return out
	}
	return paths
}

func readSecretKey(ctx context.Context, c client.Client, ns, name, key string) (string, error) {
	var sec corev1.Secret
	if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &sec); err != nil {
		return "", err
	}
	if key == "" {
		key = "RESTIC_PASSWORD"
	}
	v, ok := sec.Data[key]
	if !ok {
		return "", fmt.Errorf("secret %s/%s missing key %s", ns, name, key)
	}
	return string(v), nil
}

func secretEnvLines(ctx context.Context, c client.Client, ns, name string) ([]string, error) {
	var sec corev1.Secret
	if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &sec); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(sec.Data))
	for k, v := range sec.Data {
		out = append(out, k+"="+string(v))
	}
	return out, nil
}

func syncRepositoryToHost(ctx context.Context, c client.Client, repo *operatorv1alpha1.BackupRepository) error {
	if !repo.Spec.Backrest.SyncToHost {
		return nil
	}
	logger := log.FromContext(ctx).WithValues("backuprepository", client.ObjectKeyFromObject(repo))
	clusterNS, clusterName := resolveClusterRef(repo.Spec.Backrest.ClusterRef, repo.Namespace)
	bc, err := connectHost(ctx, c, clusterNS, clusterName)
	if err != nil {
		return err
	}
	pw, err := readSecretKey(ctx, c, repo.Namespace, repo.Spec.PasswordSecretRef.Name, keyOr(repo.Spec.PasswordSecretRef.Key, "RESTIC_PASSWORD"))
	if err != nil {
		return err
	}
	brRepo := backrest.Repo{
		ID:             repo.Name,
		URI:            repo.Spec.URL,
		Password:       pw,
		AutoInitialize: false,
		Shared:         repo.Spec.Shared,
	}
	if repo.Spec.EnvFromSecretRef != nil && repo.Spec.EnvFromSecretRef.Name != "" {
		env, err := secretEnvLines(ctx, c, repo.Namespace, repo.Spec.EnvFromSecretRef.Name)
		if err != nil {
			return err
		}
		brRepo.Env = env
	}
	if err := bc.UpsertRepo(ctx, brRepo); err != nil {
		return fmt.Errorf("upsert repo on host: %w", err)
	}
	if err := bc.IndexSnapshots(ctx, repo.Name); err != nil {
		logger.Error(err, "index snapshots after repo sync (non-fatal)")
	}
	logger.V(1).Info("synced repository to Backrest UI", "cluster", clusterNS+"/"+clusterName, "repo", repo.Name)
	return nil
}

func syncPlanToHost(ctx context.Context, c client.Client, plan *operatorv1alpha1.BackupPlan, repo *operatorv1alpha1.BackupRepository) error {
	clusterNS, clusterName := resolveClusterRef(plan.Spec.ClusterRef, plan.Namespace)
	if repo != nil && repo.Spec.Backrest.ClusterRef.Name != "" {
		clusterNS, clusterName = resolveClusterRef(repo.Spec.Backrest.ClusterRef, repo.Namespace)
	}
	bc, err := connectHost(ctx, c, clusterNS, clusterName)
	if err != nil {
		return err
	}
	repoID := plan.Spec.RepositoryRef.Name
	retention := map[string]interface{}{}
	if plan.Spec.Retention != nil {
		if v, ok := plan.Spec.Retention["keepLast"]; ok {
			retention["policyKeepLastN"] = v
		} else {
			retention = plan.Spec.Retention
		}
	}
	schedule := map[string]interface{}{"disabled": true}
	if plan.Spec.Schedule != "" {
		// Host cannot see PVC mounts; keep schedule disabled and let PVCBackup own timing.
		schedule = map[string]interface{}{"disabled": true}
	}
	bp := backrest.Plan{
		ID:        sanitizeID(plan.Namespace + "-" + plan.Name),
		Repo:      repoID,
		Paths:     orStrings(plan.Spec.Paths),
		Excludes:  orStrings(plan.Spec.Excludes),
		Schedule:  schedule,
		Retention: retention,
	}
	if err := bc.UpsertPlan(ctx, bp); err != nil {
		return err
	}
	log.FromContext(ctx).V(1).Info("synced plan to Backrest UI", "plan", bp.ID, "repo", repoID)
	return nil
}

func syncPVCBackupPlanToHost(ctx context.Context, c client.Client, b *operatorv1alpha1.PVCBackup, repo *operatorv1alpha1.BackupRepository) error {
	if repo == nil || !repo.Spec.Backrest.SyncToHost {
		return nil
	}
	clusterNS, clusterName := resolveClusterRef(repo.Spec.Backrest.ClusterRef, repo.Namespace)
	bc, err := connectHost(ctx, c, clusterNS, clusterName)
	if err != nil {
		return err
	}
	retention := map[string]interface{}{}
	if b.Spec.Retention.KeepLast != nil {
		retention["policyKeepLastN"] = *b.Spec.Retention.KeepLast
	}
	plan := backrest.Plan{
		ID:        planIDForPVCBackup(b),
		Repo:      repo.Name,
		Paths:     pvcBackupPaths(b),
		Excludes:  orStrings(b.Spec.Excludes),
		Schedule:  map[string]interface{}{"disabled": true},
		Retention: retention,
	}
	if err := bc.UpsertPlan(ctx, plan); err != nil {
		return err
	}
	if err := bc.IndexSnapshots(ctx, repo.Name); err != nil {
		log.FromContext(ctx).Error(err, "index snapshots after plan sync")
	}
	log.FromContext(ctx).V(1).Info("synced PVCBackup plan to Backrest UI", "plan", plan.ID, "repo", repo.Name)
	return nil
}

func removePVCBackupPlanFromHost(ctx context.Context, c client.Client, b *operatorv1alpha1.PVCBackup) error {
	var repo operatorv1alpha1.BackupRepository
	ns := b.Spec.RepositoryRef.Namespace
	if ns == "" {
		ns = b.Namespace
	}
	if err := c.Get(ctx, types.NamespacedName{Name: b.Spec.RepositoryRef.Name, Namespace: ns}, &repo); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if !repo.Spec.Backrest.SyncToHost {
		return nil
	}
	clusterNS, clusterName := resolveClusterRef(repo.Spec.Backrest.ClusterRef, repo.Namespace)
	bc, _, err := hostClientForCluster(ctx, c, clusterNS, clusterName)
	if err != nil {
		return err
	}
	planID := planIDForPVCBackup(b)
	if err := bc.DeletePlan(ctx, planID); err != nil {
		return err
	}
	log.FromContext(ctx).Info("removed PVCBackup plan from Backrest host", "plan", planID, "repo", repo.Name)
	return nil
}
