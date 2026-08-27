package server

import (
	"context"
	"crypto/subtle"
)

// User is one configured principal: a credential pair plus the policy that
// governs what it may do. A nil Policy means "no policy attached" -- full
// access, matching the legacy single-credential behavior. A non-nil but
// empty Policy denies everything.
type User struct {
	AccessKeyID     string  `json:"access_key_id"`
	SecretAccessKey string  `json:"secret_access_key"`
	Policy          *Policy `json:"policy,omitempty"`
}

// LookupUser returns the User matching accessKeyID, checking cfg.Users
// first and falling back to the legacy single User/Password fields
// (synthesized as a full-access User with a nil Policy) if no Users entry
// matches. It returns nil if accessKeyID matches neither.
func (cfg *Config) LookupUser(accessKeyID string) *User {
	for i := range cfg.Users {
		if subtle.ConstantTimeCompare([]byte(cfg.Users[i].AccessKeyID), []byte(accessKeyID)) == 1 {
			return &cfg.Users[i]
		}
	}
	if cfg.User != "" && subtle.ConstantTimeCompare([]byte(cfg.User), []byte(accessKeyID)) == 1 {
		return &User{AccessKeyID: cfg.User, SecretAccessKey: cfg.Password}
	}
	return nil
}

// AuthEnabled reports whether any credential is configured, legacy or
// multi-user. SigV4 and BasicAuth skip authentication entirely when false.
func (cfg *Config) AuthEnabled() bool {
	return cfg.User != "" || len(cfg.Users) > 0
}

// FilterBucketNames returns the subset of names that user is allowed to
// list (s3:ListBucket on the bucket's ARN). A nil Policy (full access,
// including the legacy single-credential user) passes every name through
// unfiltered.
func FilterBucketNames(user *User, names []string) []string {
	if user == nil || user.Policy == nil {
		return names
	}
	out := make([]string, 0, len(names))
	for _, name := range names {
		if user.Policy.Allowed("s3:ListBucket", bucketARN(name)) {
			out = append(out, name)
		}
	}
	return out
}

// userContextKey is an unexported type so context values set by this
// package can't collide with keys set elsewhere.
type userContextKey struct{}

// WithUser returns a copy of ctx carrying user, retrievable via
// UserFromContext. Used by SigV4/BasicAuth to pass the authenticated
// principal to handlers that need to filter results by policy (e.g.
// ListBuckets).
func WithUser(ctx context.Context, user *User) context.Context {
	return context.WithValue(ctx, userContextKey{}, user)
}

// UserFromContext returns the User stashed by WithUser, or nil if none was
// set (including when auth is disabled).
func UserFromContext(ctx context.Context) *User {
	user, _ := ctx.Value(userContextKey{}).(*User)
	return user
}
