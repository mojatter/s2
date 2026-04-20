package server

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func strPtr(s string) *string { return &s }

func TestInitFlags(t *testing.T) {
	testCases := []struct {
		caseName    string
		args        []string
		wantVersion bool
		wantHelp    bool
		wantConfig  string
		wantListen  *string
		wantRoot    *string
		wantBuckets *string
	}{
		{
			caseName: "no flags",
			args:     []string{"s2-server"},
		},
		{
			caseName:    "version flag",
			args:        []string{"s2-server", "-v"},
			wantVersion: true,
		},
		{
			caseName: "help flag",
			args:     []string{"s2-server", "-h"},
			wantHelp: true,
		},
		{
			caseName:   "config file flag",
			args:       []string{"s2-server", "-f", "config.json"},
			wantConfig: "config.json",
		},
		{
			caseName:   "listen flag",
			args:       []string{"s2-server", "-listen", ":8080"},
			wantListen: strPtr(":8080"),
		},
		{
			caseName: "root flag",
			args:     []string{"s2-server", "-root", "/tmp/s2"},
			wantRoot: strPtr("/tmp/s2"),
		},
		{
			caseName:    "buckets flag",
			args:        []string{"s2-server", "-buckets", "a,b,c"},
			wantBuckets: strPtr("a,b,c"),
		},
		{
			caseName:    "all config flags at once",
			args:        []string{"s2-server", "-listen", ":1", "-root", "r", "-buckets", "x"},
			wantListen:  strPtr(":1"),
			wantRoot:    strPtr("r"),
			wantBuckets: strPtr("x"),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			f, err := initFlags(tc.args)
			require.NoError(t, err)
			assert.Equal(t, tc.wantVersion, f.isVersion)
			assert.Equal(t, tc.wantHelp, f.isHelp)
			assert.Equal(t, tc.wantConfig, f.configFile)
			assert.Equal(t, tc.wantListen, f.listen)
			assert.Equal(t, tc.wantRoot, f.root)
			assert.Equal(t, tc.wantBuckets, f.buckets)
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

func TestStart(t *testing.T) {
	t.Run("returns nil on context cancel", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		addr := ln.Addr().String()
		ln.Close()

		cfg := DefaultConfig()
		cfg.Root = t.TempDir()
		cfg.Listen = addr
		cfg.ConsoleListen = ""

		srv, err := NewServer(context.Background(), cfg)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())

		errCh := make(chan error, 1)
		go func() {
			errCh <- srv.Start(ctx)
		}()

		require.Eventually(t, func() bool {
			conn, dialErr := net.Dial("tcp", addr)
			if dialErr != nil {
				return false
			}
			conn.Close()
			return true
		}, 3*time.Second, 10*time.Millisecond)

		cancel()

		select {
		case err := <-errCh:
			assert.NoError(t, err)
		case <-time.After(15 * time.Second):
			t.Fatal("Start did not return after context cancel")
		}
	})

	t.Run("returns error on port conflict", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer ln.Close()

		cfg := DefaultConfig()
		cfg.Root = t.TempDir()
		cfg.Listen = ln.Addr().String()
		cfg.ConsoleListen = ""

		srv, err := NewServer(context.Background(), cfg)
		require.NoError(t, err)

		err = srv.Start(context.Background())
		assert.Error(t, err)
	})

	t.Run("starts both S3 and console listeners when configured", func(t *testing.T) {
		freePort := func() string {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			addr := ln.Addr().String()
			require.NoError(t, ln.Close())
			return addr
		}
		s3Addr := freePort()
		consoleAddr := freePort()

		registryMux.Lock()
		origConsole := consoleHandlers
		consoleHandlers = map[string]HandlerFunc{}
		registryMux.Unlock()
		defer func() {
			registryMux.Lock()
			consoleHandlers = origConsole
			registryMux.Unlock()
		}()
		RegisterConsoleHandleFunc("GET /console-probe", func(_ *Server, w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("console"))
		})

		cfg := DefaultConfig()
		cfg.Root = t.TempDir()
		cfg.Listen = s3Addr
		cfg.ConsoleListen = consoleAddr

		srv, err := NewServer(context.Background(), cfg)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() {
			errCh <- srv.Start(ctx)
		}()

		for _, addr := range []string{s3Addr, consoleAddr} {
			require.Eventuallyf(t, func() bool {
				conn, dialErr := net.Dial("tcp", addr)
				if dialErr != nil {
					return false
				}
				conn.Close()
				return true
			}, 3*time.Second, 10*time.Millisecond, "listener %s did not come up", addr)
		}

		resp, err := http.Get("http://" + s3Addr + cfg.HealthPath)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		_ = resp.Body.Close()

		resp, err = http.Get("http://" + consoleAddr + "/console-probe")
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		_ = resp.Body.Close()

		resp, err = http.Get("http://" + s3Addr + "/console-probe")
		require.NoError(t, err)
		assert.NotEqual(t, http.StatusOK, resp.StatusCode)
		_ = resp.Body.Close()

		cancel()
		select {
		case err := <-errCh:
			assert.NoError(t, err)
		case <-time.After(15 * time.Second):
			t.Fatal("Start did not return after context cancel")
		}
	})
}

func TestInitBuckets(t *testing.T) {
	t.Run("creates buckets on startup", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Root = t.TempDir()
		cfg.Buckets = []string{"alpha", "bravo"}

		srv, err := NewServer(context.Background(), cfg)
		require.NoError(t, err)

		for _, name := range cfg.Buckets {
			if ok, _ := srv.Buckets.Exists(name); !ok {
				require.NoError(t, srv.Buckets.Create(context.Background(), name))
			}
		}

		names, err := srv.Buckets.Names()
		require.NoError(t, err)
		assert.Contains(t, names, "alpha")
		assert.Contains(t, names, "bravo")
	})

	t.Run("skips existing buckets", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Root = t.TempDir()
		cfg.Buckets = []string{"existing"}

		srv, err := NewServer(context.Background(), cfg)
		require.NoError(t, err)

		require.NoError(t, srv.Buckets.Create(context.Background(), "existing"))

		for _, name := range cfg.Buckets {
			if ok, _ := srv.Buckets.Exists(name); !ok {
				require.NoError(t, srv.Buckets.Create(context.Background(), name))
			}
		}

		names, err := srv.Buckets.Names()
		require.NoError(t, err)
		assert.Contains(t, names, "existing")
	})
}
