package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestCORSHandler(t *testing.T) {
	testCases := []struct {
		caseName       string
		method         string
		wantCode       int
		wantHandlerHit bool
	}{
		{
			caseName:       "GET passes through with CORS headers",
			method:         http.MethodGet,
			wantCode:       http.StatusOK,
			wantHandlerHit: true,
		},
		{
			caseName:       "PUT passes through with CORS headers",
			method:         http.MethodPut,
			wantCode:       http.StatusOK,
			wantHandlerHit: true,
		},
		{
			caseName:       "OPTIONS preflight returns 200 without hitting handler",
			method:         http.MethodOptions,
			wantCode:       http.StatusOK,
			wantHandlerHit: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			handlerHit := false
			next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				handlerHit = true
				w.WriteHeader(http.StatusOK)
			})

			req := httptest.NewRequest(tc.method, "/bucket/key", nil)
			w := httptest.NewRecorder()
			corsHandler(next).ServeHTTP(w, req)

			assert.Equal(t, tc.wantCode, w.Code)
			assert.Equal(t, tc.wantHandlerHit, handlerHit)
			assert.Equal(t, "*", w.Header().Get("Access-Control-Allow-Origin"))
			assert.NotEmpty(t, w.Header().Get("Access-Control-Allow-Methods"))
			assert.NotEmpty(t, w.Header().Get("Access-Control-Allow-Headers"))
		})
	}
}
