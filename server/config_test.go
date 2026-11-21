package server

import (
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

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
		{caseName: "max preview size", field: "MaxPreviewSize", got: DefaultConfig().MaxPreviewSize, want: int64(0)},
		{caseName: "effective max preview size", field: "EffectiveMaxPreviewSize", got: DefaultConfig().EffectiveMaxPreviewSize(), want: int64(10 << 20)},
		{caseName: "multipart max age", field: "MultipartMaxAge", got: DefaultConfig().MultipartMaxAge, want: int64(0)},
		{caseName: "effective multipart max age", field: "EffectiveMultipartMaxAge", got: DefaultConfig().EffectiveMultipartMaxAge(), want: DefaultMultipartMaxAge},
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

func TestEffectiveMultipartMaxAge(t *testing.T) {
	testCases := []struct {
		caseName string
		maxAge   int64
		want     time.Duration
	}{
		{caseName: "unset falls back to the default", maxAge: 0, want: DefaultMultipartMaxAge},
		{caseName: "seconds are honoured", maxAge: 90, want: 90 * time.Second},
		{caseName: "negative disables the sweeper", maxAge: -1, want: 0},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.MultipartMaxAge = tc.maxAge
			assert.Equal(t, tc.want, cfg.EffectiveMultipartMaxAge())
		})
	}
}

func TestEffectiveMaxPreviewSize(t *testing.T) {
	testCases := []struct {
		caseName string
		size     int64
		want     int64
	}{
		{caseName: "zero falls back to the default", size: 0, want: DefaultMaxPreviewSize},
		{caseName: "negative falls back to the default", size: -1, want: DefaultMaxPreviewSize},
		{caseName: "bytes are honoured", size: 123, want: 123},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.MaxPreviewSize = tc.size
			assert.Equal(t, tc.want, cfg.EffectiveMaxPreviewSize())
		})
	}
}

func TestLoadEnvMultipartMaxAge(t *testing.T) {
	testCases := []struct {
		caseName string
		env      string
		want     int64
		wantErr  bool
	}{
		{caseName: "unset keeps the config value", env: "", want: 0},
		{caseName: "seconds", env: "3600", want: 3600},
		{caseName: "negative", env: "-1", want: -1},
		{caseName: "not a number", env: "soon", wantErr: true},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			t.Setenv(EnvS2ServerMultipartMaxAge, tc.env)
			cfg := DefaultConfig()
			err := cfg.LoadEnv()
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, cfg.MultipartMaxAge)
		})
	}
}

func TestLoadLogLevel(t *testing.T) {
	testCases := []struct {
		caseName string
		file     string
		env      string
		want     slog.Level
		wantErr  bool
	}{
		{caseName: "default", want: slog.LevelInfo},
		{caseName: "file", file: `{"log_level":"debug"}`, want: slog.LevelDebug},
		{caseName: "env", env: "warn", want: slog.LevelWarn},
		{caseName: "env in upper case", env: "ERROR", want: slog.LevelError},
		{caseName: "env beats file", file: `{"log_level":"debug"}`, env: "info", want: slog.LevelInfo},
		{caseName: "invalid file", file: `{"log_level":"verbose"}`, wantErr: true},
		{caseName: "invalid env", env: "verbose", wantErr: true},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			t.Setenv(EnvS2ServerLogLevel, tc.env)
			cfg := DefaultConfig()
			var err error
			if tc.file != "" {
				path := filepath.Join(t.TempDir(), "config.json")
				require.NoError(t, os.WriteFile(path, []byte(tc.file), 0o600))
				err = cfg.LoadFile(path)
			}
			if err == nil {
				err = cfg.LoadEnv()
			}
			if tc.wantErr {
				require.Error(t, err)
				if tc.file != "" {
					assert.ErrorContains(t, err, "config.json")
				}
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, cfg.LogLevel)
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

func TestConfigValidate(t *testing.T) {
	testCases := []struct {
		caseName   string
		healthPath string
		wantErr    bool
	}{
		{caseName: "default", healthPath: "/healthz", wantErr: false},
		{caseName: "disabled", healthPath: "", wantErr: false},
		{caseName: "nested", healthPath: "/-/healthz", wantErr: false},
		{caseName: "missing leading slash", healthPath: "healthz", wantErr: true},
		{caseName: "root only", healthPath: "/", wantErr: true},
		{caseName: "relative", healthPath: "./healthz", wantErr: true},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.HealthPath = tc.healthPath
			err := cfg.Validate()
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConfigValidateUsers(t *testing.T) {
	testCases := []struct {
		caseName string
		cfg      *Config
		wantErr  bool
	}{
		{
			caseName: "valid multi-user config",
			cfg: func() *Config {
				c := DefaultConfig()
				c.Users = []User{
					{AccessKeyID: "userA", SecretAccessKey: "secretA"},
					{AccessKeyID: "userB", SecretAccessKey: "secretB", Policy: &Policy{
						Statement: []Statement{allowStatement("s3:GetObject", "*")},
					}},
				}
				return c
			}(),
			wantErr: false,
		},
		{
			caseName: "empty access key id rejected",
			cfg: func() *Config {
				c := DefaultConfig()
				c.Users = []User{{SecretAccessKey: "secretA"}}
				return c
			}(),
			wantErr: true,
		},
		{
			caseName: "empty secret access key rejected",
			cfg: func() *Config {
				c := DefaultConfig()
				c.Users = []User{{AccessKeyID: "userA"}}
				return c
			}(),
			wantErr: true,
		},
		{
			caseName: "duplicate access key id within users rejected",
			cfg: func() *Config {
				c := DefaultConfig()
				c.Users = []User{
					{AccessKeyID: "userA", SecretAccessKey: "secretA"},
					{AccessKeyID: "userA", SecretAccessKey: "secretA2"},
				}
				return c
			}(),
			wantErr: true,
		},
		{
			caseName: "access key id colliding with legacy user rejected",
			cfg: func() *Config {
				c := DefaultConfig()
				c.User = "root"
				c.Password = "rootsecret"
				c.Users = []User{{AccessKeyID: "root", SecretAccessKey: "other"}}
				return c
			}(),
			wantErr: true,
		},
		{
			caseName: "malformed nested policy rejected",
			cfg: func() *Config {
				c := DefaultConfig()
				c.Users = []User{{
					AccessKeyID:     "userA",
					SecretAccessKey: "secretA",
					Policy:          &Policy{Statement: []Statement{{Effect: "invalid"}}},
				}}
				return c
			}(),
			wantErr: true,
		},
		{
			caseName: "valid anonymous principal accepted",
			cfg: func() *Config {
				c := DefaultConfig()
				c.Users = []User{{
					AccessKeyID: AnonymousAccessKeyID,
					Policy: &Policy{
						Statement: []Statement{allowStatement("s3:GetObject", "arn:aws:s3:::public/*")},
					},
				}}
				return c
			}(),
			wantErr: false,
		},
		{
			caseName: "anonymous principal with secret_access_key rejected",
			cfg: func() *Config {
				c := DefaultConfig()
				c.Users = []User{{
					AccessKeyID:     AnonymousAccessKeyID,
					SecretAccessKey: "shouldnotbeset",
					Policy: &Policy{
						Statement: []Statement{allowStatement("s3:GetObject", "*")},
					},
				}}
				return c
			}(),
			wantErr: true,
		},
		{
			caseName: "anonymous principal without policy rejected",
			cfg: func() *Config {
				c := DefaultConfig()
				c.Users = []User{{AccessKeyID: AnonymousAccessKeyID}}
				return c
			}(),
			wantErr: true,
		},
		{
			caseName: "anonymous principal with s3:ListBucket accepted",
			cfg: func() *Config {
				c := DefaultConfig()
				c.Users = []User{{
					AccessKeyID: AnonymousAccessKeyID,
					Policy: &Policy{
						Statement: []Statement{allowStatement("s3:ListBucket", "arn:aws:s3:::public")},
					},
				}}
				return c
			}(),
			wantErr: false,
		},
		{
			// s3:ListAllMyBuckets grants nothing on its own (S3Action always
			// defers GET / to FilterBucketNames), but a Deny on it is the
			// documented escape hatch to hard-block unauthenticated root
			// bucket enumeration once s3:ListBucket is anonymously grantable
			// -- see the "Allowing anonymous directory listing" section of
			// docs/users-policy.md.
			caseName: "anonymous principal with Deny s3:ListAllMyBuckets accepted",
			cfg: func() *Config {
				c := DefaultConfig()
				c.Users = []User{{
					AccessKeyID: AnonymousAccessKeyID,
					Policy: &Policy{
						Statement: []Statement{
							allowStatement("s3:ListBucket", "arn:aws:s3:::public"),
							denyStatement("s3:ListAllMyBuckets", "arn:aws:s3:::*"),
						},
					},
				}}
				return c
			}(),
			wantErr: false,
		},
		{
			caseName: "anonymous principal with write action rejected",
			cfg: func() *Config {
				c := DefaultConfig()
				c.Users = []User{{
					AccessKeyID: AnonymousAccessKeyID,
					Policy: &Policy{
						Statement: []Statement{allowStatement("s3:PutObject", "*")},
					},
				}}
				return c
			}(),
			wantErr: true,
		},
		{
			caseName: "anonymous principal with list multipart uploads rejected",
			cfg: func() *Config {
				c := DefaultConfig()
				c.Users = []User{{
					AccessKeyID: AnonymousAccessKeyID,
					Policy: &Policy{
						Statement: []Statement{allowStatement("s3:ListBucketMultipartUploads", "*")},
					},
				}}
				return c
			}(),
			wantErr: true,
		},
		{
			caseName: "anonymous principal with list parts rejected",
			cfg: func() *Config {
				c := DefaultConfig()
				c.Users = []User{{
					AccessKeyID: AnonymousAccessKeyID,
					Policy: &Policy{
						Statement: []Statement{allowStatement("s3:ListMultipartUploadParts", "*")},
					},
				}}
				return c
			}(),
			wantErr: true,
		},
		{
			caseName: "anonymous principal with wildcard action rejected",
			cfg: func() *Config {
				c := DefaultConfig()
				c.Users = []User{{
					AccessKeyID: AnonymousAccessKeyID,
					Policy: &Policy{
						Statement: []Statement{allowStatement("*", "*")},
					},
				}}
				return c
			}(),
			wantErr: true,
		},
		{
			// A legacy User set to "*" would resolve through LookupUser's
			// fallback to a full-access (nil-Policy) User, bypassing the
			// read-only action restriction the anonymous principal is meant
			// to enforce -- see server.LookupUser and the Users-entry cases
			// above for the restriction this must not be allowed to skip.
			caseName: "legacy User set to the anonymous access key id rejected",
			cfg: func() *Config {
				c := DefaultConfig()
				c.User = AnonymousAccessKeyID
				c.Password = "somesecret"
				return c
			}(),
			wantErr: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			err := tc.cfg.Validate()
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestLoadFileOSFS(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"type": "osfs", "root": "/data", "osfs": {"confined_root": true, "no_sync": true}}`), 0o600))
	cfg := DefaultConfig()
	require.NoError(t, cfg.LoadFile(path))
	require.NotNil(t, cfg.OSFS)
	assert.True(t, cfg.OSFS.ConfinedRoot)
	assert.True(t, cfg.OSFS.NoSync)
}

func TestLoadEnvExtra(t *testing.T) {
	testCases := []struct {
		caseName string
		env      string
		want     string
		wantErr  string
	}{
		{caseName: "unset keeps the file value", env: "", want: `{"a": 1, "b": 2}`},
		{caseName: "replaces the file value whole", env: `{"a": 3}`, want: `{"a": 3}`},
		{caseName: "null replaces the file value", env: "null", want: "null"},
		{caseName: "not JSON", env: `{"a":`, wantErr: "invalid S2_SERVER_EXTRA"},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			require.NoError(t, os.WriteFile(path, []byte(`{"type": "gdrive", "extra": {"a": 1, "b": 2}}`), 0o600))
			t.Setenv(EnvS2ServerExtra, tc.env)
			cfg := DefaultConfig()
			require.NoError(t, cfg.LoadFile(path))
			err := cfg.LoadEnv()
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.JSONEq(t, tc.want, string(cfg.Extra))
		})
	}
}

func TestLoadEnvSections(t *testing.T) {
	fileJSON := `{
		"type": "osfs",
		"osfs": {"confined_root": true, "no_sync": true},
		"s3": {"endpoint_url": "http://file", "region": "file-region"},
		"azblob": {"account_name": "file-account", "account_key": "file-key"}
	}`
	fileSections := func() s2.Config {
		return s2.Config{
			OSFS:   &s2.OSFSConfig{ConfinedRoot: true, NoSync: true},
			S3:     &s2.S3Config{EndpointURL: "http://file", Region: "file-region"},
			Azblob: &s2.AzblobConfig{AccountName: "file-account", AccountKey: "file-key"},
		}
	}
	testCases := []struct {
		caseName string
		envName  string
		env      string
		want     func(*s2.Config)
		wantErr  string
	}{
		{caseName: "unset keeps the file sections", envName: EnvS2ServerOSFS, env: "", want: func(*s2.Config) {}},
		{
			caseName: "osfs replaces the file section whole",
			envName:  EnvS2ServerOSFS,
			env:      `{"no_sync": true}`,
			want:     func(c *s2.Config) { c.OSFS = &s2.OSFSConfig{NoSync: true} },
		},
		{
			caseName: "s3 replaces the file section whole",
			envName:  EnvS2ServerS3,
			env:      `{"region": "env-region"}`,
			want:     func(c *s2.Config) { c.S3 = &s2.S3Config{Region: "env-region"} },
		},
		{
			caseName: "gcs absent from the file",
			envName:  EnvS2ServerGCS,
			env:      `{"credentials_file": "/env/credentials.json"}`,
			want:     func(c *s2.Config) { c.GCS = &s2.GCSConfig{CredentialsFile: "/env/credentials.json"} },
		},
		{
			caseName: "azblob replaces the file section whole",
			envName:  EnvS2ServerAzblob,
			env:      `{"connection_string": "env-conn"}`,
			want:     func(c *s2.Config) { c.Azblob = &s2.AzblobConfig{ConnectionString: "env-conn"} },
		},
		{
			caseName: "null clears the file section",
			envName:  EnvS2ServerOSFS,
			env:      "null",
			want:     func(c *s2.Config) { c.OSFS = nil },
		},
		{caseName: "not JSON", envName: EnvS2ServerOSFS, env: `{"no_sync":`, wantErr: "invalid S2_SERVER_OSFS"},
		{caseName: "not an object", envName: EnvS2ServerS3, env: `[]`, wantErr: "invalid S2_SERVER_S3"},
		{caseName: "wrong field type", envName: EnvS2ServerAzblob, env: `{"account_name": 1}`, wantErr: "invalid S2_SERVER_AZBLOB"},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			require.NoError(t, os.WriteFile(path, []byte(fileJSON), 0o600))
			for _, name := range []string{EnvS2ServerOSFS, EnvS2ServerS3, EnvS2ServerGCS, EnvS2ServerAzblob} {
				t.Setenv(name, "")
			}
			t.Setenv(tc.envName, tc.env)
			cfg := DefaultConfig()
			require.NoError(t, cfg.LoadFile(path))
			err := cfg.LoadEnv()
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			want := fileSections()
			tc.want(&want)
			assert.Equal(t, want.OSFS, cfg.OSFS)
			assert.Equal(t, want.S3, cfg.S3)
			assert.Equal(t, want.GCS, cfg.GCS)
			assert.Equal(t, want.Azblob, cfg.Azblob)
		})
	}
}
