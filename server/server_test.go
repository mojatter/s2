package server

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func strPtr(s string) *string { return &s }

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
	noop := func(_ *Server, _ http.ResponseWriter, _ *http.Request) {}

	testCases := []struct {
		caseName     string
		registry     *map[string]HandlerFunc
		register     func(string, HandlerFunc)
		wantPanicMsg string
	}{
		{
			caseName:     "S3 duplicate panics",
			registry:     &s3Handlers,
			register:     RegisterS3HandleFunc,
			wantPanicMsg: "s2: S3 handler already registered for GET /test",
		},
		{
			caseName:     "console duplicate panics",
			registry:     &consoleHandlers,
			register:     RegisterConsoleHandleFunc,
			wantPanicMsg: "s2: console handler already registered for GET /test",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			// Save and restore the specific registry under test so we
			// don't stomp on routes that real init() calls registered.
			handlersMux.Lock()
			original := *tc.registry
			*tc.registry = map[string]HandlerFunc{}
			handlersMux.Unlock()
			defer func() {
				handlersMux.Lock()
				*tc.registry = original
				handlersMux.Unlock()
			}()

			tc.register("GET /test", noop)
			assert.PanicsWithValue(t, tc.wantPanicMsg, func() {
				tc.register("GET /test", noop)
			})
		})
	}
}

func TestS3HandlerAndConsoleHandler(t *testing.T) {
	// Snapshot and reset both split registries so the test doesn't leak
	// into (or get polluted by) real init-time registrations.
	handlersMux.Lock()
	origS3 := s3Handlers
	origConsole := consoleHandlers
	s3Handlers = map[string]HandlerFunc{}
	consoleHandlers = map[string]HandlerFunc{}
	handlersMux.Unlock()
	defer func() {
		handlersMux.Lock()
		s3Handlers = origS3
		consoleHandlers = origConsole
		handlersMux.Unlock()
	}()

	RegisterS3HandleFunc("GET /s3-only", func(_ *Server, w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("s3"))
	})
	RegisterConsoleHandleFunc("GET /console-only", func(_ *Server, w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("console"))
	})

	cfg := DefaultConfig()
	cfg.Root = t.TempDir()
	srv, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)

	t.Run("S3Handler serves only S3 routes", func(t *testing.T) {
		h := srv.S3Handler()
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", "/s3-only", nil))
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "s3", w.Body.String())

		// Console routes are not served here.
		w = httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", "/console-only", nil))
		assert.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("ConsoleHandler serves only console routes", func(t *testing.T) {
		h := srv.ConsoleHandler()
		require.NotNil(t, h)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", "/console-only", nil))
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "console", w.Body.String())

		w = httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", "/s3-only", nil))
		assert.Equal(t, http.StatusNotFound, w.Code)
	})
}

func TestConsoleHandlerNilWhenEmpty(t *testing.T) {
	handlersMux.Lock()
	origConsole := consoleHandlers
	consoleHandlers = map[string]HandlerFunc{}
	handlersMux.Unlock()
	defer func() {
		handlersMux.Lock()
		consoleHandlers = origConsole
		handlersMux.Unlock()
	}()

	cfg := DefaultConfig()
	cfg.Root = t.TempDir()
	srv, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)

	assert.Nil(t, srv.ConsoleHandler(), "ConsoleHandler should be nil when no console routes are registered")
}

func TestStart(t *testing.T) {
	t.Run("returns nil on context cancel", func(t *testing.T) {
		// Pick a free port upfront so we know what to dial
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		addr := ln.Addr().String()
		ln.Close()

		cfg := DefaultConfig()
		cfg.Root = t.TempDir()
		cfg.Listen = addr
		cfg.ConsoleListen = "" // keep the test focused on the S3 listener

		srv, err := NewServer(context.Background(), cfg)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())

		errCh := make(chan error, 1)
		go func() {
			errCh <- srv.Start(ctx)
		}()

		// Wait for server to be listening
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
		// Occupy a port
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
		// Pick two free ports upfront.
		freePort := func() string {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			addr := ln.Addr().String()
			require.NoError(t, ln.Close())
			return addr
		}
		s3Addr := freePort()
		consoleAddr := freePort()

		// Register a minimal console route so ConsoleHandler is non-nil
		// for this test, and restore the registry afterwards so we don't
		// leak into other tests.
		handlersMux.Lock()
		origConsole := consoleHandlers
		consoleHandlers = map[string]HandlerFunc{}
		handlersMux.Unlock()
		defer func() {
			handlersMux.Lock()
			consoleHandlers = origConsole
			handlersMux.Unlock()
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

		// Wait for both listeners.
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

		// Health endpoint should answer on the S3 listener.
		resp, err := http.Get("http://" + s3Addr + cfg.HealthPath)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		_ = resp.Body.Close()

		// Console probe should answer on the console listener, not on the S3 one.
		resp, err = http.Get("http://" + consoleAddr + "/console-probe")
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		_ = resp.Body.Close()

		resp, err = http.Get("http://" + s3Addr + "/console-probe")
		require.NoError(t, err)
		// S3 listener routes "/console-probe" as a bucket operation: the
		// bucket does not exist, so the SigV4-wrapped handler returns a
		// 403/404-class S3 error. We only need to confirm the console
		// handler itself is *not* served here.
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

func TestRenderConsoleIndex(t *testing.T) {
	testCases := []struct {
		caseName     string
		buckets      []string
		wantContains []string
	}{
		{
			caseName:     "no buckets renders index page",
			wantContains: []string{`id="main-content"`},
		},
		{
			caseName:     "bucket names appear in rendered page",
			buckets:      []string{"alpha", "bravo"},
			wantContains: []string{`id="main-content"`, "alpha", "bravo"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Root = t.TempDir()
			srv, err := NewServer(context.Background(), cfg)
			require.NoError(t, err)

			for _, name := range tc.buckets {
				require.NoError(t, srv.Buckets.Create(context.Background(), name))
			}

			w := httptest.NewRecorder()
			require.NoError(t, srv.RenderConsoleIndex(w, nil))

			body := w.Body.String()
			for _, want := range tc.wantContains {
				assert.Contains(t, body, want)
			}
		})
	}
}

func TestInitBuckets(t *testing.T) {
	t.Run("creates buckets on startup", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Root = t.TempDir()
		cfg.Buckets = []string{"alpha", "bravo"}

		srv, err := NewServer(context.Background(), cfg)
		require.NoError(t, err)

		// Simulate the init-buckets logic from Run
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

		// Pre-create the bucket
		require.NoError(t, srv.Buckets.Create(context.Background(), "existing"))

		// Run init logic again — should not error
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

