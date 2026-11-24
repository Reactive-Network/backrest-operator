package backrest

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Reactive-Network/backrest-operator/internal/logging"
	ctrl "sigs.k8s.io/controller-runtime"
)

const defaultTimeout = 120 * time.Second

// Client talks to a Backrest host over ConnectRPC JSON (HTTP POST /v1.Backrest/...).
// With Credentials set, every request carries HTTP Basic auth; Backrest checks it
// when its config has auth enabled and ignores it while auth is disabled.
type Client struct {
	BaseURL     string
	HTTPClient  *http.Client
	Credentials Credentials
}

// NewClient builds a client with credentials from BACKREST_AUTH_USERNAME /
// BACKREST_AUTH_PASSWORD when both are set (see CredentialsFromEnv).
func NewClient(baseURL string) *Client {
	return &Client{
		BaseURL: strings.TrimRight(baseURL, "/"),
		HTTPClient: &http.Client{
			Timeout: defaultTimeout,
		},
		Credentials: CredentialsFromEnv(),
	}
}

// WithCredentials replaces the client's Basic credentials when cr is complete.
func (c *Client) WithCredentials(cr Credentials) *Client {
	if !cr.Empty() {
		c.Credentials = cr
	}
	return c
}

// HostURL builds the in-cluster Service URL for a BackrestCluster.
func HostURL(namespace, clusterName string) string {
	return fmt.Sprintf("http://backrest-host-%s.%s.svc:9898", clusterName, namespace)
}

// PlanTag / InstanceTag match Backrest's restic tagging convention.
func PlanTag(planID string) string       { return "plan:" + planID }
func InstanceTag(instance string) string { return "created-by:" + instance }

type Repo struct {
	ID             string   `json:"id"`
	URI            string   `json:"uri"`
	Password       string   `json:"password,omitempty"`
	Env            []string `json:"env,omitempty"`
	Flags          []string `json:"flags,omitempty"`
	AutoInitialize bool     `json:"autoInitialize,omitempty"`
	Shared         bool     `json:"shared,omitempty"`
	GUID           string   `json:"guid,omitempty"`
}

type Plan struct {
	ID        string                 `json:"id"`
	Repo      string                 `json:"repo"`
	Paths     []string               `json:"paths,omitempty"`
	Excludes  []string               `json:"excludes,omitempty"`
	Schedule  map[string]interface{} `json:"schedule,omitempty"`
	Retention map[string]interface{} `json:"retention,omitempty"`
}

type Config struct {
	Modno    int                    `json:"modno,omitempty"`
	Version  int                    `json:"version,omitempty"`
	Instance string                 `json:"instance,omitempty"`
	Repos    []Repo                 `json:"repos,omitempty"`
	Plans    []Plan                 `json:"plans,omitempty"`
	Auth     *Auth                  `json:"auth,omitempty"`
	Sync     map[string]interface{} `json:"sync,omitempty"`
}

func (c *Client) post(ctx context.Context, method string, body any, out any) error {
	log := ctrl.LoggerFrom(ctx).WithName("backrest").WithValues("method", method)
	var buf bytes.Buffer
	if body == nil {
		buf.WriteString("{}")
	} else if err := json.NewEncoder(&buf).Encode(body); err != nil {
		return err
	}
	reqBytes := buf.Bytes()
	log.V(1).Info("backrest request", "bytes", len(reqBytes), "body", logging.BodySummary(reqBytes, 240))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/v1.Backrest/"+method, bytes.NewReader(reqBytes))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if !c.Credentials.Empty() {
		req.SetBasicAuth(c.Credentials.Username, c.Credentials.Password)
	}
	started := time.Now()
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		log.Error(err, "backrest call failed")
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return err
	}
	elapsed := time.Since(started).Round(time.Millisecond)
	if resp.StatusCode >= 300 {
		// Keep error messages short — full body only at debug.
		log.V(1).Info("backrest error response", "status", resp.StatusCode, "duration", elapsed, "body", logging.BodySummary(data, 240))
		return fmt.Errorf("backrest %s: HTTP %d: %s", method, resp.StatusCode, logging.Truncate(string(data), 200))
	}
	log.V(1).Info("backrest ok", "status", resp.StatusCode, "duration", elapsed, "bytes", len(data), "body", logging.BodySummary(data, 240))
	if out == nil || len(data) == 0 {
		return nil
	}
	return json.Unmarshal(data, out)
}

func (c *Client) GetConfig(ctx context.Context) (*Config, error) {
	var cfg Config
	if err := c.post(ctx, "GetConfig", map[string]any{}, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Client) SetConfig(ctx context.Context, cfg *Config) (*Config, error) {
	var out Config
	if err := c.post(ctx, "SetConfig", cfg, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// EnsureInstance sets Config.instance when empty or mismatched.
func (c *Client) EnsureInstance(ctx context.Context, instance string) error {
	return c.EnsureHostConfig(ctx, instance, nil)
}

// EnsureHostConfig sets Config.instance and, when auth is non-nil, the auth block,
// in a single SetConfig that is skipped when nothing changed.
func (c *Client) EnsureHostConfig(ctx context.Context, instance string, auth *AuthPolicy) error {
	if instance == "" {
		return fmt.Errorf("instance is required")
	}
	cfg, err := c.GetConfig(ctx)
	if err != nil {
		return err
	}
	changed := false
	if cfg.Instance != instance {
		cfg.Instance = instance
		changed = true
	}
	authChanged, err := auth.Apply(cfg)
	if err != nil {
		return fmt.Errorf("backrest auth: %w", err)
	}
	if !changed && !authChanged {
		return nil
	}
	_, err = c.SetConfig(ctx, cfg)
	return err
}

// UpsertRepo adds or updates a repository. Caller must EnsureInstance first.
func (c *Client) UpsertRepo(ctx context.Context, repo Repo) error {
	cfg, err := c.GetConfig(ctx)
	if err != nil {
		return err
	}
	if cfg.Instance == "" {
		return fmt.Errorf("backrest instance is not set; call EnsureInstance first")
	}
	for i, existing := range cfg.Repos {
		if existing.ID == repo.ID {
			if repo.GUID == "" {
				repo.GUID = existing.GUID
			}
			cfg.Repos[i] = repo
			_, err = c.SetConfig(ctx, cfg)
			return err
		}
	}
	return c.post(ctx, "AddRepo", map[string]any{"repo": repo}, &Config{})
}

// UpsertPlan merges a plan into the host config.
func (c *Client) UpsertPlan(ctx context.Context, plan Plan) error {
	cfg, err := c.GetConfig(ctx)
	if err != nil {
		return err
	}
	if cfg.Instance == "" {
		return fmt.Errorf("backrest instance is not set")
	}
	if plan.Schedule == nil {
		plan.Schedule = map[string]interface{}{"disabled": true}
	}
	found := false
	for i, p := range cfg.Plans {
		if p.ID == plan.ID {
			cfg.Plans[i] = plan
			found = true
			break
		}
	}
	if !found {
		cfg.Plans = append(cfg.Plans, plan)
	}
	_, err = c.SetConfig(ctx, cfg)
	return err
}

// DeletePlan removes a plan from the host config by ID (no-op if absent).
func (c *Client) DeletePlan(ctx context.Context, planID string) error {
	if planID == "" {
		return nil
	}
	cfg, err := c.GetConfig(ctx)
	if err != nil {
		return err
	}
	if cfg.Instance == "" {
		return fmt.Errorf("backrest instance is not set")
	}
	kept := cfg.Plans[:0]
	found := false
	for _, p := range cfg.Plans {
		if p.ID == planID {
			found = true
			continue
		}
		kept = append(kept, p)
	}
	if !found {
		return nil
	}
	cfg.Plans = kept
	_, err = c.SetConfig(ctx, cfg)
	return err
}

func (c *Client) IndexSnapshots(ctx context.Context, repoID string) error {
	return c.post(ctx, "DoRepoTask", map[string]any{
		"repoId": repoID,
		"task":   "TASK_INDEX_SNAPSHOTS",
	}, nil)
}

func (c *Client) ListSnapshots(ctx context.Context, repoID, planID string) ([]map[string]any, error) {
	body := map[string]any{"repoId": repoID}
	if planID != "" {
		body["planId"] = planID
	}
	var out struct {
		Snapshots []map[string]any `json:"snapshots"`
	}
	if err := c.post(ctx, "ListSnapshots", body, &out); err != nil {
		return nil, err
	}
	return out.Snapshots, nil
}

// GetOperations returns Backrest operation history matching the optional selector fields.
func (c *Client) GetOperations(ctx context.Context, selector map[string]any, lastN int) ([]map[string]any, error) {
	body := map[string]any{}
	if selector != nil {
		body["selector"] = selector
	}
	if lastN > 0 {
		body["lastN"] = lastN
	}
	var out struct {
		Operations []map[string]any `json:"operations"`
	}
	if err := c.post(ctx, "GetOperations", body, &out); err != nil {
		return nil, err
	}
	return out.Operations, nil
}

// GetDownloadURL returns a relative signed download path (e.g. ./download/<jwt>/)
// for an indexed snapshot operation. filePath "/" dumps the whole snapshot as .tar.
func (c *Client) GetDownloadURL(ctx context.Context, opID int64, filePath string) (string, error) {
	if filePath == "" {
		filePath = "/"
	}
	var out struct {
		Value string `json:"value"`
	}
	if err := c.post(ctx, "GetDownloadURL", map[string]any{
		"opId":     opID,
		"filePath": filePath,
	}, &out); err != nil {
		return "", err
	}
	if out.Value == "" {
		return "", fmt.Errorf("empty download URL")
	}
	return out.Value, nil
}

// AbsoluteDownloadURL joins a Backrest relative ./download/... URL with a public base.
func AbsoluteDownloadURL(publicBase, relative string) string {
	base := strings.TrimRight(strings.TrimSpace(publicBase), "/")
	rel := strings.TrimSpace(relative)
	rel = strings.TrimPrefix(rel, "./")
	if !strings.HasPrefix(rel, "/") {
		rel = "/" + rel
	}
	if base == "" {
		return rel
	}
	return base + rel
}

// DownloadLink is a minted Backrest GetDownloadURL result.
type DownloadLink struct {
	DownloadURL string
	RelativeURL string
	OperationID int64
	Path        string
	ExpiresAt   time.Time
	Mode        string // restore | stream
}

// Restore schedules a Backrest restore task and returns the operation id.
func (c *Client) Restore(ctx context.Context, planID, repoID, snapshotID, path, target string) (int64, error) {
	if path == "" {
		path = "/"
	}
	if target == "" {
		return 0, fmt.Errorf("restore target is required")
	}
	var out struct {
		OperationID json.Number `json:"operationId"`
	}
	if err := c.post(ctx, "Restore", map[string]any{
		"planId":     planID,
		"repoId":     repoID,
		"snapshotId": snapshotID,
		"path":       path,
		"target":     target,
	}, &out); err != nil {
		return 0, err
	}
	id, err := out.OperationID.Int64()
	if err != nil || id == 0 {
		return 0, fmt.Errorf("restore returned empty operationId")
	}
	return id, nil
}

// GetOperation returns a single operation by id (via selector.operationId).
func (c *Client) GetOperation(ctx context.Context, opID int64) (map[string]any, error) {
	ops, err := c.GetOperations(ctx, map[string]any{"operationId": opID}, 1)
	if err != nil {
		return nil, err
	}
	for _, op := range ops {
		if jsonNumberAsInt64(op["id"]) == opID {
			return op, nil
		}
	}
	// Some Backrest builds ignore operationId selector — scan lastN.
	ops, err = c.GetOperations(ctx, nil, 50)
	if err != nil {
		return nil, err
	}
	for _, op := range ops {
		if jsonNumberAsInt64(op["id"]) == opID {
			return op, nil
		}
	}
	return nil, fmt.Errorf("operation %d not found", opID)
}

// WaitOperation waits until the operation reaches a terminal status.
func (c *Client) WaitOperation(ctx context.Context, opID int64, timeout time.Duration) (map[string]any, error) {
	if timeout <= 0 {
		timeout = 10 * time.Minute
	}
	deadline := time.Now().Add(timeout)
	for {
		op, err := c.GetOperation(ctx, opID)
		if err != nil {
			return nil, err
		}
		status, _ := op["status"].(string)
		switch status {
		case "STATUS_SUCCESS", "STATUS_WARNING":
			return op, nil
		case "STATUS_ERROR", "STATUS_SYSTEM_CANCELLED", "STATUS_USER_CANCELLED":
			msg, _ := op["displayMessage"].(string)
			if msg == "" {
				msg = status
			}
			return op, fmt.Errorf("operation %d failed: %s", opID, msg)
		}
		if time.Now().After(deadline) {
			return op, fmt.Errorf("operation %d still %s after %s", opID, status, timeout)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// ResolvePlanID finds planId from an indexed snapshot operation.
func (c *Client) ResolvePlanID(ctx context.Context, repoID, snapshotID string) (string, error) {
	selector := map[string]any{"snapshotId": snapshotID}
	if repoID != "" {
		selector["repoId"] = repoID
	}
	ops, err := c.GetOperations(ctx, selector, 20)
	if err != nil {
		return "", err
	}
	for _, op := range ops {
		if _, ok := op["operationIndexSnapshot"]; !ok {
			continue
		}
		if plan, _ := op["planId"].(string); plan != "" {
			return plan, nil
		}
	}
	return "", fmt.Errorf("no indexed snapshot with planId for snapshot_id=%s (run index_repository)", snapshotID)
}

// MintDownloadURL mints a signed download URL.
// mode "restore" (default): schedule Backrest Restore (shows in UI) then GetDownloadURL.
// mode "stream": GetDownloadURL from indexed snapshot only (no Restore in UI).
func (c *Client) MintDownloadURL(ctx context.Context, repoID, snapshotID, planID, path, publicBase, mode, restoreTarget string) (*DownloadLink, error) {
	if snapshotID == "" {
		return nil, fmt.Errorf("snapshotID is required")
	}
	if path == "" {
		path = "/"
	}
	if mode == "" || mode == "restore" {
		if planID == "" {
			var err error
			planID, err = c.ResolvePlanID(ctx, repoID, snapshotID)
			if err != nil {
				return nil, err
			}
		}
		if restoreTarget == "" {
			restoreTarget = fmt.Sprintf("/data/snapdl/%s-%d", snapshotID[:min(8, len(snapshotID))], time.Now().Unix())
		}
		opID, err := c.Restore(ctx, planID, repoID, snapshotID, path, restoreTarget)
		if err != nil {
			return nil, fmt.Errorf("backrest Restore: %w", err)
		}
		if _, err := c.WaitOperation(ctx, opID, 15*time.Minute); err != nil {
			return nil, err
		}
		rel, err := c.GetDownloadURL(ctx, opID, "/")
		if err != nil {
			return nil, err
		}
		link := &DownloadLink{
			DownloadURL: AbsoluteDownloadURL(publicBase, rel),
			RelativeURL: rel,
			OperationID: opID,
			Path:        path,
			Mode:        "restore",
		}
		if exp, ok := ExpiryFromDownloadRelativeURL(rel); ok {
			link.ExpiresAt = exp
		}
		return link, nil
	}

	// stream mode — index snapshot dump
	selector := map[string]any{"snapshotId": snapshotID}
	if planID != "" {
		selector["planId"] = planID
	}
	if repoID != "" {
		selector["repoId"] = repoID
	}
	ops, err := c.GetOperations(ctx, selector, 20)
	if err != nil {
		return nil, err
	}
	var opID int64
	for _, op := range ops {
		if _, ok := op["operationIndexSnapshot"]; !ok {
			continue
		}
		opID = jsonNumberAsInt64(op["id"])
		if opID > 0 {
			break
		}
	}
	if opID == 0 {
		return nil, fmt.Errorf("no indexed snapshot operation for snapshot_id=%s (run index_repository first)", snapshotID)
	}
	rel, err := c.GetDownloadURL(ctx, opID, path)
	if err != nil {
		return nil, err
	}
	link := &DownloadLink{
		DownloadURL: AbsoluteDownloadURL(publicBase, rel),
		RelativeURL: rel,
		OperationID: opID,
		Path:        path,
		Mode:        "stream",
	}
	if exp, ok := ExpiryFromDownloadRelativeURL(rel); ok {
		link.ExpiresAt = exp
	}
	return link, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ExpiryFromDownloadRelativeURL parses JWT exp from ./download/<jwt>/ when present.
func ExpiryFromDownloadRelativeURL(relative string) (time.Time, bool) {
	rel := strings.Trim(strings.TrimSpace(relative), "/")
	rel = strings.TrimPrefix(rel, "./")
	parts := strings.Split(rel, "/")
	var jwt string
	for _, p := range parts {
		if strings.Count(p, ".") == 2 && len(p) > 20 {
			jwt = p
			break
		}
	}
	if jwt == "" {
		return time.Time{}, false
	}
	segs := strings.Split(jwt, ".")
	if len(segs) != 3 {
		return time.Time{}, false
	}
	payload, err := decodeJWTSegment(segs[1])
	if err != nil {
		return time.Time{}, false
	}
	var claims struct {
		Exp int64 `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil || claims.Exp == 0 {
		return time.Time{}, false
	}
	return time.Unix(claims.Exp, 0).UTC(), true
}

func decodeJWTSegment(seg string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(seg)
}

func jsonNumberAsInt64(v interface{}) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int64:
		return n
	case int:
		return int64(n)
	case json.Number:
		i, _ := n.Int64()
		return i
	case string:
		var i int64
		fmt.Sscanf(n, "%d", &i)
		return i
	default:
		return 0
	}
}

func (c *Client) ClearRepoHistory(ctx context.Context, repoID string) error {
	return c.post(ctx, "ClearHistory", map[string]any{
		"selector":   map[string]any{"repoId": repoID},
		"onlyFailed": false,
	}, nil)
}
