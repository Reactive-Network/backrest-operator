package backrest

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestHashPasswordIsBase64Bcrypt(t *testing.T) {
	enc, err := HashPassword("s3cret")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := base64.StdEncoding.DecodeString(enc)
	if err != nil {
		t.Fatalf("not base64: %v", err)
	}
	if !strings.HasPrefix(string(raw), "$2a$") {
		t.Fatalf("not a bcrypt hash: %q", raw)
	}
	if !PasswordMatches(enc, "s3cret") {
		t.Fatal("password should match its own hash")
	}
	if PasswordMatches(enc, "other") || PasswordMatches("not-base64!", "s3cret") || PasswordMatches(enc, "") {
		t.Fatal("unexpected match")
	}
	if _, err := HashPassword(""); err == nil {
		t.Fatal("empty password must fail")
	}
}

func TestAuthPolicyApply(t *testing.T) {
	op := Credentials{Username: "operator", Password: "pw1"}

	// nil policy leaves the config alone.
	cfg := &Config{Auth: &Auth{Disabled: true}}
	if changed, err := (*AuthPolicy)(nil).Apply(cfg); err != nil || changed {
		t.Fatalf("nil policy: changed=%v err=%v", changed, err)
	}

	// Disabled on a default config is a no-op.
	if changed, _ := (&AuthPolicy{}).Apply(cfg); changed {
		t.Fatal("disabled on disabled config must not change it")
	}
	// Disabled with users left over resets to {disabled: true}.
	cfg = &Config{Auth: &Auth{Users: []User{{Name: "x", PasswordBcrypt: "y"}}}}
	if changed, _ := (&AuthPolicy{}).Apply(cfg); !changed || !cfg.Auth.Disabled || len(cfg.Auth.Users) != 0 {
		t.Fatalf("disable: changed=%v auth=%+v", changed, cfg.Auth)
	}

	// Enabling writes one bcrypt user.
	enabled := &AuthPolicy{Enabled: true, Users: []Credentials{op}}
	cfg = &Config{Auth: &Auth{Disabled: true}}
	changed, err := enabled.Apply(cfg)
	if err != nil || !changed {
		t.Fatalf("enable: changed=%v err=%v", changed, err)
	}
	if cfg.Auth.Disabled || len(cfg.Auth.Users) != 1 || cfg.Auth.Users[0].Name != "operator" {
		t.Fatalf("enable: auth=%+v", cfg.Auth)
	}
	stored := cfg.Auth.Users[0].PasswordBcrypt
	if !PasswordMatches(stored, "pw1") {
		t.Fatal("stored hash does not match")
	}

	// Same password again: hash kept, nothing changes (no SetConfig, no modno bump).
	if changed, _ := enabled.Apply(cfg); changed || cfg.Auth.Users[0].PasswordBcrypt != stored {
		t.Fatal("second apply must be a no-op")
	}

	// Password rotation rehashes.
	rotated := &AuthPolicy{Enabled: true, Users: []Credentials{{Username: "operator", Password: "pw2"}}}
	if changed, _ := rotated.Apply(cfg); !changed || !PasswordMatches(cfg.Auth.Users[0].PasswordBcrypt, "pw2") {
		t.Fatal("rotation must rehash")
	}

	// A user added by hand in the UI is removed.
	cfg.Auth.Users = append(cfg.Auth.Users, User{Name: "intruder", PasswordBcrypt: stored})
	if changed, _ := rotated.Apply(cfg); !changed || len(cfg.Auth.Users) != 1 || cfg.Auth.Users[0].Name != "operator" {
		t.Fatalf("extra user must be dropped: %+v", cfg.Auth.Users)
	}

	// Invalid policies.
	if _, err := (&AuthPolicy{Enabled: true}).Apply(&Config{}); err == nil {
		t.Fatal("enabled without users must fail")
	}
	if _, err := (&AuthPolicy{Enabled: true, Users: []Credentials{{Username: "u"}}}).Apply(&Config{}); err == nil {
		t.Fatal("user without password must fail")
	}
	if _, err := (&AuthPolicy{Enabled: true, Users: []Credentials{op, op}}).Apply(&Config{}); err == nil {
		t.Fatal("duplicate user must fail")
	}
}

func TestCredentialsFromEnv(t *testing.T) {
	t.Setenv(EnvAuthUsername, " operator ")
	t.Setenv(EnvAuthPassword, "pw")
	c := NewClient("http://example")
	if c.Credentials.Username != "operator" || c.Credentials.Password != "pw" {
		t.Fatalf("got %+v", c.Credentials)
	}
	t.Setenv(EnvAuthPassword, "")
	if !NewClient("http://example").Credentials.Empty() {
		t.Fatal("half-set env must give empty credentials")
	}
}

// fakeHost mimics the parts of Backrest the client uses: GetConfig/SetConfig with modno
// checks, and RequireAuthentication (Basic auth checked only while auth is enabled).
type fakeHost struct {
	mu       sync.Mutex
	cfg      Config
	setCalls int
	authHdrs []string
}

func (f *fakeHost) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.authHdrs = append(f.authHdrs, r.Header.Get("Authorization"))
	if f.cfg.Auth != nil && !f.cfg.Auth.Disabled {
		u, p, ok := r.BasicAuth()
		allowed := false
		for _, usr := range f.cfg.Auth.Users {
			if ok && usr.Name == u && PasswordMatches(usr.PasswordBcrypt, p) {
				allowed = true
			}
		}
		if !allowed {
			http.Error(w, "Unauthorized (No Authorization Header)", http.StatusUnauthorized)
			return
		}
	}
	switch r.URL.Path {
	case "/v1.Backrest/GetConfig":
		_ = json.NewEncoder(w).Encode(f.cfg)
	case "/v1.Backrest/SetConfig":
		var in Config
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if in.Modno != f.cfg.Modno {
			http.Error(w, "config modno mismatch", http.StatusBadRequest)
			return
		}
		if in.Auth != nil && !in.Auth.Disabled && len(in.Auth.Users) == 0 {
			http.Error(w, "auth enabled but no users", http.StatusBadRequest)
			return
		}
		in.Modno++
		f.cfg = in
		f.setCalls++
		_ = json.NewEncoder(w).Encode(f.cfg)
	default:
		http.NotFound(w, r)
	}
}

func TestClientEnablesAuthAndKeepsWorking(t *testing.T) {
	host := &fakeHost{cfg: Config{Version: 6, Auth: &Auth{Disabled: true}}}
	srv := httptest.NewServer(host)
	defer srv.Close()

	t.Setenv(EnvAuthUsername, "")
	t.Setenv(EnvAuthPassword, "")
	creds := Credentials{Username: "operator", Password: "pw1"}
	bc := NewClient(srv.URL).WithCredentials(creds)
	policy := &AuthPolicy{Enabled: true, Users: []Credentials{creds}}

	ctx := context.Background()
	if err := bc.EnsureHostConfig(ctx, "main", policy); err != nil {
		t.Fatal(err)
	}
	if host.setCalls != 1 || host.cfg.Instance != "main" || host.cfg.Auth.Disabled {
		t.Fatalf("after enable: sets=%d cfg=%+v", host.setCalls, host.cfg)
	}
	// Auth is now on: the same client keeps working, a client without credentials does not.
	if err := bc.EnsureHostConfig(ctx, "main", policy); err != nil {
		t.Fatalf("second sync with credentials: %v", err)
	}
	if host.setCalls != 1 {
		t.Fatalf("unchanged config must not be written again, sets=%d", host.setCalls)
	}
	if _, err := NewClient(srv.URL).GetConfig(ctx); err == nil || !strings.Contains(err.Error(), "HTTP 401") {
		t.Fatalf("client without credentials: err=%v", err)
	}
	wrong := NewClient(srv.URL).WithCredentials(Credentials{Username: "operator", Password: "nope"})
	if _, err := wrong.GetConfig(ctx); err == nil {
		t.Fatal("wrong password must be rejected")
	}
	for _, h := range host.authHdrs {
		if h != "" && !strings.HasPrefix(h, "Basic ") {
			t.Fatalf("unexpected Authorization header %q", h)
		}
	}

	// Switching auth off again through the authenticated client.
	if err := bc.EnsureHostConfig(ctx, "main", &AuthPolicy{}); err != nil {
		t.Fatal(err)
	}
	if !host.cfg.Auth.Disabled {
		t.Fatalf("auth must be disabled: %+v", host.cfg.Auth)
	}
	if _, err := NewClient(srv.URL).GetConfig(ctx); err != nil {
		t.Fatalf("anonymous client after disable: %v", err)
	}
}

func TestClientSendsNoAuthWithoutCredentials(t *testing.T) {
	host := &fakeHost{cfg: Config{Instance: "main", Auth: &Auth{Disabled: true}}}
	srv := httptest.NewServer(host)
	defer srv.Close()
	t.Setenv(EnvAuthUsername, "")
	t.Setenv(EnvAuthPassword, "")
	if _, err := NewClient(srv.URL).GetConfig(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(host.authHdrs) != 1 || host.authHdrs[0] != "" {
		t.Fatalf("expected no Authorization header, got %q", host.authHdrs)
	}
}
