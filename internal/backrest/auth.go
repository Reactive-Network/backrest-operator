package backrest

import (
	"encoding/base64"
	"fmt"
	"os"
	"strings"

	"golang.org/x/crypto/bcrypt"
)

// Environment variables that give a Backrest API client HTTP Basic credentials.
// The Helm chart fills them from the Secret named by BackrestCluster.spec.auth.existingSecret.
const (
	EnvAuthUsername = "BACKREST_AUTH_USERNAME"
	EnvAuthPassword = "BACKREST_AUTH_PASSWORD"
)

// Keys of the Secret referenced by BackrestCluster.spec.auth.existingSecret.
const (
	SecretKeyUsername = "username"
	SecretKeyPassword = "password"
)

// Auth mirrors Backrest's v1.Auth config block (config.proto).
type Auth struct {
	Disabled bool   `json:"disabled,omitempty"`
	Users    []User `json:"users,omitempty"`
}

// User mirrors Backrest's v1.User. PasswordBcrypt is base64(bcrypt(password)),
// the encoding Backrest's checkPassword expects.
type User struct {
	Name           string `json:"name"`
	PasswordBcrypt string `json:"passwordBcrypt,omitempty"`
}

// Credentials are HTTP Basic credentials for the Backrest API.
type Credentials struct {
	Username string
	Password string
}

// Empty reports whether the credentials are unusable (either half missing).
func (c Credentials) Empty() bool {
	return c.Username == "" || c.Password == ""
}

// CredentialsFromEnv reads BACKREST_AUTH_USERNAME / BACKREST_AUTH_PASSWORD.
func CredentialsFromEnv() Credentials {
	return Credentials{
		Username: strings.TrimSpace(os.Getenv(EnvAuthUsername)),
		Password: os.Getenv(EnvAuthPassword),
	}
}

// HashPassword returns the value Backrest stores in User.passwordBcrypt.
func HashPassword(password string) (string, error) {
	if password == "" {
		return "", fmt.Errorf("empty password")
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", fmt.Errorf("bcrypt: %w", err)
	}
	return base64.StdEncoding.EncodeToString(hash), nil
}

// PasswordMatches reports whether a stored passwordBcrypt value matches password.
func PasswordMatches(encoded, password string) bool {
	if encoded == "" || password == "" {
		return false
	}
	hash, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return false
	}
	return bcrypt.CompareHashAndPassword(hash, []byte(password)) == nil
}

// AuthPolicy is the auth block the operator owns on a Backrest host.
// Enabled=false writes {disabled: true}. Enabled=true writes exactly Users
// (other users, e.g. ones added by hand in the UI, are removed).
type AuthPolicy struct {
	Enabled bool
	Users   []Credentials
}

// Apply makes cfg.Auth match the policy and reports whether it changed anything.
// A stored hash is kept while it still matches the password, so a sync does not
// rewrite the config (and bump modno) every time.
func (p *AuthPolicy) Apply(cfg *Config) (bool, error) {
	if p == nil || cfg == nil {
		return false, nil
	}
	if !p.Enabled {
		if cfg.Auth != nil && cfg.Auth.Disabled && len(cfg.Auth.Users) == 0 {
			return false, nil
		}
		cfg.Auth = &Auth{Disabled: true}
		return true, nil
	}
	if len(p.Users) == 0 {
		return false, fmt.Errorf("auth enabled but no users")
	}
	existing := map[string]string{}
	if cfg.Auth != nil {
		for _, u := range cfg.Auth.Users {
			existing[u.Name] = u.PasswordBcrypt
		}
	}
	users := make([]User, 0, len(p.Users))
	seen := map[string]struct{}{}
	changed := cfg.Auth == nil || cfg.Auth.Disabled || len(cfg.Auth.Users) != len(p.Users)
	for i, cr := range p.Users {
		if cr.Empty() {
			return false, fmt.Errorf("auth user %d: username and password are required", i)
		}
		if _, dup := seen[cr.Username]; dup {
			return false, fmt.Errorf("auth user %q listed twice", cr.Username)
		}
		seen[cr.Username] = struct{}{}
		if stored, ok := existing[cr.Username]; ok && PasswordMatches(stored, cr.Password) {
			users = append(users, User{Name: cr.Username, PasswordBcrypt: stored})
			if !changed && cfg.Auth.Users[i].Name != cr.Username {
				changed = true
			}
			continue
		}
		hash, err := HashPassword(cr.Password)
		if err != nil {
			return false, fmt.Errorf("auth user %q: %w", cr.Username, err)
		}
		users = append(users, User{Name: cr.Username, PasswordBcrypt: hash})
		changed = true
	}
	if !changed {
		return false, nil
	}
	cfg.Auth = &Auth{Users: users}
	return true, nil
}
