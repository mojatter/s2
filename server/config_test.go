package server

import (
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
		{caseName: "root", field: "Root", got: DefaultConfig().Root, want: "/var/lib/s2"},
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
