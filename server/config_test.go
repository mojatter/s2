package server

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mojatter/s2"
)

func TestLoadEnvBuckets(t *testing.T) {
	t.Setenv(EnvS2ServerBuckets, "foo,bar,baz")
	cfg := DefaultConfig()
	require.NoError(t, cfg.LoadEnv())
	assert.Equal(t, []string{"foo", "bar", "baz"}, cfg.Buckets)
}

func TestDefaultConfig(t *testing.T) {
	testCases := []struct {
		caseName string
		field    string
		got      any
		want     any
	}{
		{caseName: "listen", field: "Listen", got: DefaultConfig().Listen, want: ":9000"},
		{caseName: "type", field: "Type", got: string(DefaultConfig().Type), want: "osfs"},
		{caseName: "root", field: "Root", got: DefaultConfig().Root, want: DefaultRoot},
		// MaxUploadSize is intentionally 0 in DefaultConfig; EffectiveMaxUploadSize resolves a backend-specific default.
		{caseName: "max upload size", field: "MaxUploadSize", got: DefaultConfig().MaxUploadSize, want: int64(0)},
		{caseName: "effective max upload size (osfs)", field: "EffectiveMaxUploadSize", got: DefaultConfig().EffectiveMaxUploadSize(), want: int64(5 << 30)},
		{caseName: "max preview size", field: "MaxPreviewSize", got: DefaultConfig().MaxPreviewSize, want: int64(10 << 20)},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.got)
		})
	}
}

func TestEffectiveMaxUploadSize(t *testing.T) {
	testCases := []struct {
		caseName string
		cfg      *Config
		want     int64
	}{
		{
			caseName: "osfs default",
			cfg:      DefaultConfig(),
			want:     DefaultMaxUploadSize,
		},
		{
			caseName: "memfs default",
			cfg: func() *Config {
				c := DefaultConfig()
				c.Type = s2.TypeMemFS
				return c
			}(),
			want: DefaultMemfsMaxUploadSize,
		},
		{
			caseName: "explicit override on memfs",
			cfg: func() *Config {
				c := DefaultConfig()
				c.Type = s2.TypeMemFS
				c.MaxUploadSize = 1 << 30
				return c
			}(),
			want: 1 << 30,
		},
		{
			caseName: "explicit override on osfs",
			cfg: func() *Config {
				c := DefaultConfig()
				c.MaxUploadSize = 123
				return c
			}(),
			want: 123,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.cfg.EffectiveMaxUploadSize())
		})
	}
}

// TestConfigPrecedence verifies that configuration sources are applied in
// the documented order: default < file < env < flag. The test exercises the
// same load sequence Run uses (DefaultConfig -> LoadFile -> LoadEnv -> flag
// override) without actually starting an HTTP server.
func TestConfigPrecedence(t *testing.T) {
	// Write a small config file with a root value we can later see being
	// overridden.
	dir := t.TempDir()
	fileRoot := filepath.Join(dir, "from-file")
	cfgPath := filepath.Join(dir, "config.json")
	body, err := json.Marshal(map[string]any{
		"root":   fileRoot,
		"listen": ":7001",
	})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(cfgPath, body, 0o600))

	testCases := []struct {
		caseName string
		envRoot  string
		flagRoot *string
		wantRoot string
	}{
		{
			caseName: "default only (no file, no env, no flag)",
			wantRoot: DefaultConfig().Root,
		},
		{
			caseName: "file beats default",
			wantRoot: fileRoot,
		},
		{
			caseName: "env beats file",
			envRoot:  "/from-env",
			wantRoot: "/from-env",
		},
		{
			caseName: "flag beats env",
			envRoot:  "/from-env",
			flagRoot: strPtr("/from-flag"),
			wantRoot: "/from-flag",
		},
		{
			caseName: "flag beats file (no env)",
			flagRoot: strPtr("/from-flag"),
			wantRoot: "/from-flag",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			if tc.envRoot != "" {
				t.Setenv(EnvS2ServerRoot, tc.envRoot)
			} else {
				t.Setenv(EnvS2ServerRoot, "")
			}

			cfg := DefaultConfig()
			// "default only" uses no file; every other case loads the file.
			if tc.caseName != "default only (no file, no env, no flag)" {
				require.NoError(t, cfg.LoadFile(cfgPath))
			}
			require.NoError(t, cfg.LoadEnv())
			if tc.flagRoot != nil {
				cfg.Root = *tc.flagRoot
			}

			assert.Equal(t, tc.wantRoot, cfg.Root)
		})
	}
}
