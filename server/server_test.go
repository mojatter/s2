package server

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInitFlags(t *testing.T) {
	testCases := []struct {
		caseName   string
		args       []string
		wantVersion bool
		wantHelp    bool
		wantConfig  string
	}{
		{
			caseName:   "no flags",
			args:       []string{"s2-server"},
			wantVersion: false,
			wantHelp:    false,
			wantConfig:  "",
		},
		{
			caseName:   "version flag",
			args:       []string{"s2-server", "-v"},
			wantVersion: true,
		},
		{
			caseName:   "help flag",
			args:       []string{"s2-server", "-h"},
			wantHelp:   true,
		},
		{
			caseName:   "config file flag",
			args:       []string{"s2-server", "-f", "config.json"},
			wantConfig: "config.json",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			f, err := initFlags(tc.args)
			require.NoError(t, err)
			assert.Equal(t, tc.wantVersion, f.isVersion)
			assert.Equal(t, tc.wantHelp, f.isHelp)
			assert.Equal(t, tc.wantConfig, f.configFile)
		})
	}
}

func TestNewServer(t *testing.T) {
	t.Run("sets StartedAt", func(t *testing.T) {
		before := time.Now()
		cfg := DefaultConfig()
		cfg.Root = t.TempDir()
		srv, err := NewServer(context.Background(), cfg)
		after := time.Now()

		require.NoError(t, err)
		assert.False(t, srv.StartedAt.Before(before))
		assert.False(t, srv.StartedAt.After(after))
	})

	t.Run("sets Config", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Root = t.TempDir()
		srv, err := NewServer(context.Background(), cfg)

		require.NoError(t, err)
		assert.Equal(t, cfg, srv.Config)
	})

	t.Run("initializes Buckets", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Root = t.TempDir()
		srv, err := NewServer(context.Background(), cfg)

		require.NoError(t, err)
		assert.NotNil(t, srv.Buckets)
	})

	t.Run("initializes Template", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Root = t.TempDir()
		srv, err := NewServer(context.Background(), cfg)

		require.NoError(t, err)
		assert.NotNil(t, srv.Template)
	})
}

func TestRegisterHandleFunc(t *testing.T) {
	t.Run("duplicate panics", func(t *testing.T) {
		// Save and restore global state
		handlersMux.Lock()
		original := handlers
		handlers = map[string]HandlerFunc{}
		handlersMux.Unlock()
		defer func() {
			handlersMux.Lock()
			handlers = original
			handlersMux.Unlock()
		}()

		noop := func(_ *Server, _ http.ResponseWriter, _ *http.Request) {}
		RegisterHandleFunc("GET /test", noop)

		assert.PanicsWithValue(t, "s2: handler already registered for GET /test", func() {
			RegisterHandleFunc("GET /test", noop)
		})
	})
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
		{caseName: "max upload size", field: "MaxUploadSize", got: DefaultConfig().MaxUploadSize, want: int64(5 << 30)},
		{caseName: "max preview size", field: "MaxPreviewSize", got: DefaultConfig().MaxPreviewSize, want: int64(10 << 20)},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.got)
		})
	}
}
