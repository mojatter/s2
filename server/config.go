package server

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/mojatter/s2"
)

// splitBucketList parses a comma-separated bucket list (used by both the
// S2_SERVER_BUCKETS env var and the -buckets flag). Whitespace around each
// entry is trimmed; empty entries are dropped so "a,,b" becomes ["a","b"]
// and the zero-value "" becomes a nil slice rather than [""].
func splitBucketList(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := parts[:0]
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

const (
	EnvS2ServerConfig         = "S2_SERVER_CONFIG"
	EnvS2ServerListen         = "S2_SERVER_LISTEN"
	EnvS2ServerConsoleListen  = "S2_SERVER_CONSOLE_LISTEN"
	EnvS2ServerHealthPath     = "S2_SERVER_HEALTH_PATH"
	EnvS2ServerType           = "S2_SERVER_TYPE"
	EnvS2ServerRoot           = "S2_SERVER_ROOT"
	EnvS2ServerMaxUploadSize  = "S2_SERVER_MAX_UPLOAD_SIZE"
	EnvS2ServerMaxPreviewSize = "S2_SERVER_MAX_PREVIEW_SIZE"
	EnvS2ServerUser           = "S2_SERVER_USER"
	EnvS2ServerPassword       = "S2_SERVER_PASSWORD" // #nosec G101 -- env var name, not a credential
	EnvS2ServerBuckets        = "S2_SERVER_BUCKETS"
)

// Config is a configuration for the server.
type Config struct {
	s2.Config
	// Listen is the address the S3-compatible API listens on.
	Listen string `json:"listen"`
	// ConsoleListen is the address a dedicated Web Console listener will
	// bind to in a future release. It is reserved here so forward-looking
	// configurations can be written today; the current server still
	// serves the console from the same listener as the S3 API when this
	// value is empty (the default).
	ConsoleListen string `json:"console_listen"`
	// HealthPath reserves a future location for the health check endpoint
	// on the S3 listener. It is read and validated today but not yet
	// mounted; the existing "/healthz" handler remains untouched in this
	// release so that operators with probes pointing at it keep working.
	HealthPath string `json:"health_path"`
	// MaxUploadSize is the maximum upload size in bytes. When 0, a backend-specific
	// default is used (see EffectiveMaxUploadSize): 5 GiB for osfs/s3, 16 MiB for
	// memfs. The conservative memfs default protects the host from accidental
	// OOM when a large upload targets the in-memory backend; set an explicit
	// value here to override.
	MaxUploadSize int64 `json:"max_upload_size"`
	// MaxPreviewSize is the maximum file size for text preview in bytes (0 = default 10MB).
	MaxPreviewSize int64 `json:"max_preview_size"`
	// User is the username for authentication (Basic Auth for Web Console, Access Key ID for S3 API).
	// When empty, authentication is disabled.
	User string `json:"user"`
	// Password is the password for authentication (Basic Auth password for Web Console, Secret Access Key for S3 API).
	Password string `json:"password"`
	// Buckets is a list of bucket names to create on startup if they don't already exist.
	Buckets []string `json:"buckets"`
}

const (
	DefaultMaxUploadSize      = 5 << 30  // 5 GiB — default for osfs/s3 backends.
	DefaultMemfsMaxUploadSize = 16 << 20 // 16 MiB — conservative default for the in-memory backend.
	DefaultMaxPreviewSize     = 10 << 20 // 10 MiB
)

// DefaultRoot is the default storage root path used by DefaultConfig when
// the user does not supply one via -root, -f, or S2_SERVER_ROOT.
//
// It is intentionally a var (not a const) so that binaries can be built with
// a different default via linker flags, matching the same idiom used for
// version injection:
//
//	go build -ldflags "-X github.com/mojatter/s2/server.DefaultRoot=/var/lib/s2" ./cmd/s2-server
//
// This lets the stock "go install" binary default to a relative "data"
// directory (so a new user can run s2-server in any directory and
// immediately see where data lands) while the Docker image is built with
// /var/lib/s2 baked in.
var DefaultRoot = "data"

// EffectiveMaxUploadSize returns the upload size limit to enforce for this
// configuration. When MaxUploadSize is explicitly set (> 0) it is returned
// as-is; otherwise a backend-specific default is chosen so that switching
// Type to memfs does not silently inherit the 5 GiB default.
func (cfg *Config) EffectiveMaxUploadSize() int64 {
	if cfg.MaxUploadSize > 0 {
		return cfg.MaxUploadSize
	}
	if cfg.Type == s2.TypeMemFS {
		return DefaultMemfsMaxUploadSize
	}
	return DefaultMaxUploadSize
}

func DefaultConfig() *Config {
	return &Config{
		Config: s2.Config{
			Type: s2.TypeOSFS,
			Root: DefaultRoot,
		},
		Listen: ":9000",
		// MaxUploadSize intentionally left 0 — EffectiveMaxUploadSize resolves
		// a backend-appropriate default at request time.
		MaxPreviewSize: DefaultMaxPreviewSize,
	}
}

func (cfg *Config) LoadFile(filename string) error {
	data, err := os.ReadFile(filepath.Clean(filename))
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, cfg); err != nil {
		return err
	}
	return nil
}

func (cfg *Config) LoadEnv() error {
	if listen := os.Getenv(EnvS2ServerListen); listen != "" {
		cfg.Listen = listen
	}
	if v, ok := os.LookupEnv(EnvS2ServerConsoleListen); ok {
		// LookupEnv so the operator can explicitly disable the console
		// listener with an empty value, distinct from "unset".
		cfg.ConsoleListen = v
	}
	if v, ok := os.LookupEnv(EnvS2ServerHealthPath); ok {
		cfg.HealthPath = v
	}
	if typ := os.Getenv(EnvS2ServerType); typ != "" {
		cfg.Type = s2.Type(typ)
	}
	if root := os.Getenv(EnvS2ServerRoot); root != "" {
		cfg.Root = root
	}
	if v := os.Getenv(EnvS2ServerMaxUploadSize); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid %s: %w", EnvS2ServerMaxUploadSize, err)
		}
		cfg.MaxUploadSize = n
	}
	if v := os.Getenv(EnvS2ServerMaxPreviewSize); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid %s: %w", EnvS2ServerMaxPreviewSize, err)
		}
		cfg.MaxPreviewSize = n
	}
	if v := os.Getenv(EnvS2ServerUser); v != "" {
		cfg.User = v
	}
	if v := os.Getenv(EnvS2ServerPassword); v != "" {
		cfg.Password = v
	}
	if v := os.Getenv(EnvS2ServerBuckets); v != "" {
		cfg.Buckets = splitBucketList(v)
	}
	return nil
}
