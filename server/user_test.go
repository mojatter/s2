package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigLookupUser(t *testing.T) {
	cfg := &Config{
		User:     "root",
		Password: "rootsecret",
		Users: []User{
			{AccessKeyID: "userA", SecretAccessKey: "secretA"},
			{AccessKeyID: "userB", SecretAccessKey: "secretB", Policy: &Policy{}},
		},
	}

	testCases := []struct {
		caseName    string
		accessKeyID string
		wantNil     bool
		wantSecret  string
		wantPolicy  bool
	}{
		{caseName: "finds multi-user entry", accessKeyID: "userA", wantSecret: "secretA"},
		{caseName: "finds multi-user entry with policy", accessKeyID: "userB", wantSecret: "secretB", wantPolicy: true},
		{caseName: "falls back to legacy user", accessKeyID: "root", wantSecret: "rootsecret"},
		{caseName: "unknown access key returns nil", accessKeyID: "nope", wantNil: true},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			u := cfg.LookupUser(tc.accessKeyID)
			if tc.wantNil {
				assert.Nil(t, u)
				return
			}
			if assert.NotNil(t, u) {
				assert.Equal(t, tc.wantSecret, u.SecretAccessKey)
				assert.Equal(t, tc.wantPolicy, u.Policy != nil)
			}
		})
	}
}

func TestConfigLookupUserNoLegacy(t *testing.T) {
	cfg := &Config{Users: []User{{AccessKeyID: "userA", SecretAccessKey: "secretA"}}}
	assert.Nil(t, cfg.LookupUser("unknown"))
}

func TestConfigAuthEnabled(t *testing.T) {
	testCases := []struct {
		caseName string
		cfg      *Config
		want     bool
	}{
		{caseName: "nothing configured", cfg: &Config{}, want: false},
		{caseName: "legacy user set", cfg: &Config{User: "root"}, want: true},
		{caseName: "users set", cfg: &Config{Users: []User{{AccessKeyID: "a", SecretAccessKey: "b"}}}, want: true},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.cfg.AuthEnabled())
		})
	}
}

func TestFilterBucketNames(t *testing.T) {
	names := []string{"public", "private", "other"}
	restricted := &User{Policy: &Policy{Statement: []Statement{
		allowStatement("s3:ListBucket", "arn:aws:s3:::public"),
	}}}

	testCases := []struct {
		caseName string
		user     *User
		want     []string
	}{
		{caseName: "nil user passes through unfiltered", user: nil, want: names},
		{caseName: "nil policy passes through unfiltered", user: &User{}, want: names},
		{caseName: "restrictive policy filters", user: restricted, want: []string{"public"}},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			assert.Equal(t, tc.want, FilterBucketNames(tc.user, names))
		})
	}
}
