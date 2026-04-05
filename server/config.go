package server

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/mojatter/s2"
)

const (
	EnvS2ServerConfig         = "S2_SERVER_CONFIG"
	EnvS2ServerListen         = "S2_SERVER_LISTEN"
	EnvS2ServerType           = "S2_SERVER_TYPE"
	EnvS2ServerRoot           = "S2_SERVER_ROOT"
	EnvS2ServerMaxUploadSize  = "S2_SERVER_MAX_UPLOAD_SIZE"
	EnvS2ServerMaxPreviewSize = "S2_SERVER_MAX_PREVIEW_SIZE"
)

// Config is a configuration for the server.
type Config struct {
	s2.Config
	// Listen is the address to listen on.
	Listen string `json:"listen"`
	// MaxUploadSize is the maximum upload size in bytes (0 = default 5GB).
	MaxUploadSize int64 `json:"max_upload_size"`
	// MaxPreviewSize is the maximum file size for text preview in bytes (0 = default 10MB).
	MaxPreviewSize int64 `json:"max_preview_size"`
}

const (
	DefaultMaxUploadSize  = 5 << 30  // 5GB
	DefaultMaxPreviewSize = 10 << 20 // 10MB
)

func DefaultConfig() *Config {
	return &Config{
		Config: s2.Config{
			Type: s2.TypeOSFS,
			Root: "/var/lib/s2",
		},
		Listen:         ":9000",
		MaxUploadSize:  DefaultMaxUploadSize,
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
	return nil
}
