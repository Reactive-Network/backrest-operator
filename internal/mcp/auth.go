package mcp

import (
	"context"
	"fmt"

	authnv1 "k8s.io/api/authentication/v1"
	authzv1 "k8s.io/api/authorization/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const StdioUsername = "mcp-stdio-local"

var DestructiveTools = map[string]struct{}{
	"delete_repository": {},
	"delete_plan":       {},
	"delete_snapshot":   {},
}

// tool -> (verb, resource, group)
var toolPermissions = map[string][3]string{
	"list_clusters":      {"list", "backrestclusters", "operator.backrest.io"},
	"get_cluster":        {"get", "backrestclusters", "operator.backrest.io"},
	"list_repositories":  {"list", "backuprepositories", "operator.backrest.io"},
	"get_repository":     {"get", "backuprepositories", "operator.backrest.io"},
	"create_repository":  {"create", "backuprepositories", "operator.backrest.io"},
	"update_repository":  {"update", "backuprepositories", "operator.backrest.io"},
	"delete_repository":  {"delete", "backuprepositories", "operator.backrest.io"},
	"list_plans":         {"list", "backupplans", "operator.backrest.io"},
	"get_plan":           {"get", "backupplans", "operator.backrest.io"},
	"create_plan":        {"create", "backupplans", "operator.backrest.io"},
	"update_plan":        {"update", "backupplans", "operator.backrest.io"},
	"delete_plan":        {"delete", "backupplans", "operator.backrest.io"},
	"trigger_backup":     {"update", "pvcbackups", "operator.backrest.io"},
	"list_snapshots":             {"get", "backuprepositories", "operator.backrest.io"},
	"get_snapshot_download_url":  {"get", "backuprepositories", "operator.backrest.io"},
	"create_snapshot_download":   {"create", "snapshotdownloads", "operator.backrest.io"},
	"get_snapshot_download":      {"get", "snapshotdownloads", "operator.backrest.io"},
	"get_snapshot":               {"get", "backuprepositories", "operator.backrest.io"},
	"delete_snapshot":            {"delete", "backuprepositories", "operator.backrest.io"},
	"get_host_config":            {"get", "backrestclusters", "operator.backrest.io"},
	"index_repository":           {"update", "backuprepositories", "operator.backrest.io"},
	"create_pvc_backup":          {"create", "pvcbackups", "operator.backrest.io"},
	"get_pvc_backup":             {"get", "pvcbackups", "operator.backrest.io"},
	"create_pvc_restore":         {"create", "pvcrestores", "operator.backrest.io"},
	"get_pvc_restore":            {"get", "pvcrestores", "operator.backrest.io"},
	"repo_status":                {"get", "backuprepositories", "operator.backrest.io"},
}

type UserIdentity struct {
	Username string
	Groups   []string
	UID      string
}

type Auth struct {
	clientset kubernetes.Interface
	onDeny    func(tool string)
}

func NewAuth(cfg *rest.Config, onDeny func(tool string)) (*Auth, error) {
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	if onDeny == nil {
		onDeny = func(string) {}
	}
	return &Auth{clientset: cs, onDeny: onDeny}, nil
}

// NewAuthForTest builds Auth without a Kubernetes client (unit tests for gating only).
func NewAuthForTest(onDeny func(tool string)) *Auth {
	if onDeny == nil {
		onDeny = func(string) {}
	}
	return &Auth{onDeny: onDeny}
}

func (a *Auth) ReviewToken(ctx context.Context, token string) (*UserIdentity, error) {
	tr, err := a.clientset.AuthenticationV1().TokenReviews().Create(ctx, &authnv1.TokenReview{
		Spec: authnv1.TokenReviewSpec{Token: token},
	}, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}
	if !tr.Status.Authenticated || tr.Status.User.Username == "" {
		return nil, fmt.Errorf("token not authenticated")
	}
	return &UserIdentity{
		Username: tr.Status.User.Username,
		Groups:   tr.Status.User.Groups,
		UID:      tr.Status.User.UID,
	}, nil
}

func (a *Auth) SubjectAccessReview(ctx context.Context, user *UserIdentity, namespace, verb, group, resource string) bool {
	if a.clientset == nil {
		return false
	}
	sar, err := a.clientset.AuthorizationV1().SubjectAccessReviews().Create(ctx, &authzv1.SubjectAccessReview{
		Spec: authzv1.SubjectAccessReviewSpec{
			User:   user.Username,
			Groups: user.Groups,
			ResourceAttributes: &authzv1.ResourceAttributes{
				Namespace: namespace,
				Verb:      verb,
				Group:     group,
				Resource:  resource,
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		return false
	}
	return sar.Status.Allowed
}

func (a *Auth) AuthorizeTool(ctx context.Context, user *UserIdentity, tool, namespace string, allowDestructive bool, args map[string]interface{}) bool {
	if _, destructive := DestructiveTools[tool]; destructive && !allowDestructive {
		a.onDeny(tool)
		return false
	}
	perm, ok := toolPermissions[tool]
	if !ok {
		a.onDeny(tool)
		return false
	}
	// trigger_backup: force-run existing needs update; create needs create.
	if tool == "trigger_backup" && args != nil {
		if name, _ := args["name"].(string); name != "" {
			perm = [3]string{"update", "pvcbackups", "operator.backrest.io"}
		} else {
			perm = [3]string{"create", "pvcbackups", "operator.backrest.io"}
		}
	}
	if user.Username == StdioUsername {
		return true
	}
	if !a.SubjectAccessReview(ctx, user, namespace, perm[0], perm[2], perm[1]) {
		a.onDeny(tool)
		return false
	}
	return true
}
