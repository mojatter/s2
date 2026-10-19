package server

import (
	"cmp"
	"context"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

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
			registryMux.Lock()
			original := *tc.registry
			*tc.registry = map[string]HandlerFunc{}
			registryMux.Unlock()
			defer func() {
				registryMux.Lock()
				*tc.registry = original
				registryMux.Unlock()
			}()

			tc.register("GET /test", noop)
			assert.PanicsWithValue(t, tc.wantPanicMsg, func() {
				tc.register("GET /test", noop)
			})
		})
	}
}

func TestS3HandlerAndConsoleHandler(t *testing.T) {
	registryMux.Lock()
	origS3 := s3Handlers
	origConsole := consoleHandlers
	s3Handlers = map[string]HandlerFunc{}
	consoleHandlers = map[string]HandlerFunc{}
	registryMux.Unlock()
	defer func() {
		registryMux.Lock()
		s3Handlers = origS3
		consoleHandlers = origConsole
		registryMux.Unlock()
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
		assert.Equal(t, "*", w.Header().Get("Access-Control-Expose-Headers"))
		assert.Empty(t, w.Header().Get("Access-Control-Allow-Credentials"))

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
			// Access-Control-Allow-Headers: "*" alone does not cover the
			// Authorization request header per the Fetch spec, so it must
			// be listed explicitly for browser SigV4 requests to work.
			assert.Contains(t, w.Header().Get("Access-Control-Allow-Headers"), "Authorization")
			assert.Equal(t, "*", w.Header().Get("Access-Control-Expose-Headers"))
			// Expose-Headers "*" turns into a literal name once credentials are allowed.
			assert.Empty(t, w.Header().Get("Access-Control-Allow-Credentials"))
		})
	}
}

func TestCleanPathHandler(t *testing.T) {
	testCases := []struct {
		caseName       string
		target         string
		wantCode       int
		wantHandlerHit bool
	}{
		{
			caseName:       "an ordinary key passes through",
			target:         "/bucket/key.txt",
			wantCode:       http.StatusOK,
			wantHandlerHit: true,
		},
		{
			caseName:       "a name that merely starts with dots passes through",
			target:         "/bucket/..hidden.txt",
			wantCode:       http.StatusOK,
			wantHandlerHit: true,
		},
		{
			caseName:       "a trailing slash passes through",
			target:         "/bucket/",
			wantCode:       http.StatusOK,
			wantHandlerHit: true,
		},
		{
			caseName:       "an encoded dot element is rejected",
			target:         "/bucket/.%2Fkey.txt",
			wantCode:       http.StatusBadRequest,
			wantHandlerHit: false,
		},
		{
			caseName:       "an encoded empty element is rejected",
			target:         "/bucket/a%2F%2Fb.txt",
			wantCode:       http.StatusBadRequest,
			wantHandlerHit: false,
		},
		{
			caseName:       "an encoded parent element in the key is rejected",
			target:         "/bucket1/..%2Fbucket2%2Fsecret.txt",
			wantCode:       http.StatusBadRequest,
			wantHandlerHit: false,
		},
		{
			caseName:       "a dot-encoded parent element is rejected",
			target:         "/bucket1/%2e%2e%2Fbucket2%2Fsecret.txt",
			wantCode:       http.StatusBadRequest,
			wantHandlerHit: false,
		},
		{
			caseName:       "an encoded parent element in the bucket is rejected",
			target:         "/..%2Fescaped/key.txt",
			wantCode:       http.StatusBadRequest,
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

			reject := func(w http.ResponseWriter, r *http.Request) {
				WriteS3Error(w, r, "InvalidArgument", "The request path is not valid", http.StatusBadRequest)
			}
			req := httptest.NewRequest(http.MethodGet, tc.target, nil)
			w := httptest.NewRecorder()
			cleanPathHandler(reject)(next).ServeHTTP(w, req)

			assert.Equal(t, tc.wantCode, w.Code)
			assert.Equal(t, tc.wantHandlerHit, handlerHit)
			if !tc.wantHandlerHit {
				assert.Contains(t, w.Body.String(), "InvalidArgument")
			}
		})
	}
}

// Each mux rejects a traversal in the shape its own clients expect.
func TestCleanPathHandlerPerMux(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Root = t.TempDir()
	srv, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)

	const target = "/bucket1/..%2Fbucket2%2Fsecret.txt"

	t.Run("the S3 API answers an XML error document", func(t *testing.T) {
		w := httptest.NewRecorder()
		srv.S3Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, target, nil))
		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Header().Get("Content-Type"), "xml")
		assert.Contains(t, w.Body.String(), "InvalidArgument")
	})

	t.Run("the console answers plain text", func(t *testing.T) {
		w := httptest.NewRecorder()
		srv.ConsoleHandler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, target, nil))
		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.NotContains(t, w.Body.String(), "<Error>")
	})
}

func TestHealthHandler(t *testing.T) {
	testCases := []struct {
		caseName    string
		path        string
		reqPath     string
		reqMethod   string
		wantBody    string
		wantHandled bool
	}{
		{
			caseName:    "empty path is a no-op middleware",
			path:        "",
			reqPath:     "/healthz",
			reqMethod:   http.MethodGet,
			wantBody:    "next",
			wantHandled: true,
		},
		{
			caseName:  "GET matching path returns ok",
			path:      "/healthz",
			reqPath:   "/healthz",
			reqMethod: http.MethodGet,
			wantBody:  "ok",
		},
		{
			caseName:  "HEAD matching path returns ok",
			path:      "/healthz",
			reqPath:   "/healthz",
			reqMethod: http.MethodHead,
			wantBody:  "ok",
		},
		{
			caseName:    "POST matching path passes through",
			path:        "/healthz",
			reqPath:     "/healthz",
			reqMethod:   http.MethodPost,
			wantBody:    "next",
			wantHandled: true,
		},
		{
			caseName:    "non-matching path passes through",
			path:        "/healthz",
			reqPath:     "/other",
			reqMethod:   http.MethodGet,
			wantBody:    "next",
			wantHandled: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			handled := false
			next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				handled = true
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("next"))
			})

			w := httptest.NewRecorder()
			req := httptest.NewRequest(tc.reqMethod, tc.reqPath, nil)
			healthHandler(tc.path)(next).ServeHTTP(w, req)

			assert.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, tc.wantHandled, handled)
			if tc.reqMethod != http.MethodHead {
				assert.Equal(t, tc.wantBody, w.Body.String())
			}
		})
	}
}

func TestStartSkipsNilFactories(t *testing.T) {
	// Swap out the factory list so real init-time factories don't run.
	registryMux.Lock()
	origFuncs := httpServerFactories
	httpServerFactories = nil
	registryMux.Unlock()
	defer func() {
		registryMux.Lock()
		httpServerFactories = origFuncs
		registryMux.Unlock()
	}()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	nilCalled := false
	realCalled := false
	registerHttpServerFactory(func(_ *Server) *http.Server {
		nilCalled = true
		return nil
	})
	registerHttpServerFactory(func(_ *Server) *http.Server {
		realCalled = true
		return &http.Server{
			Addr:              addr,
			ReadHeaderTimeout: 30 * time.Second,
		}
	})

	cfg := DefaultConfig()
	cfg.Root = t.TempDir()
	srv, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Start(ctx)
	}()

	// The non-nil factory's listener should come up.
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

	// Asserted after <-errCh: the channel receive establishes happens-before
	// with the factory writes inside Start, so these reads are race-free.
	assert.True(t, nilCalled, "nil-returning factory should still be invoked")
	assert.True(t, realCalled, "non-nil factory should be invoked")
}

func TestRequestLogHandler(t *testing.T) {
	testCases := []struct {
		caseName   string
		method     string
		handler    http.HandlerFunc
		wantStatus int
		wantBytes  string
		wantBodyID bool
	}{
		{
			caseName: "error response carries the ID",
			handler: func(w http.ResponseWriter, r *http.Request) {
				WriteS3Error(w, r, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)
			},
			wantStatus: http.StatusNotFound,
			wantBodyID: true,
		},
		{
			caseName: "write",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte("hello"))
			},
			wantStatus: http.StatusOK,
			wantBytes:  "bytes=5 ",
		},
		{
			caseName: "read from",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusPartialContent)
				_, _ = io.Copy(w, io.LimitReader(strings.NewReader("hello world"), 7))
			},
			wantStatus: http.StatusPartialContent,
			wantBytes:  "bytes=7 ",
		},
		{
			caseName: "operation",
			handler: func(_ http.ResponseWriter, r *http.Request) {
				SetS3Operation(r, "UploadPart", "upload_id", "u1", "part_number", "3")
			},
			wantStatus: http.StatusOK,
			wantBytes:  "operation=UploadPart upload_id=u1 part_number=3 status=200 bytes=0 ",
		},
		{
			caseName: "head discards the body",
			method:   http.MethodHead,
			handler: func(w http.ResponseWriter, r *http.Request) {
				WriteS3Error(w, r, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)
			},
			wantStatus: http.StatusNotFound,
			wantBytes:  "bytes=0 ",
		},
		{
			caseName:   "nothing written",
			handler:    func(http.ResponseWriter, *http.Request) {},
			wantStatus: http.StatusOK,
			wantBytes:  "bytes=0 ",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			buf := captureSlog(t)
			w := httptest.NewRecorder()
			method := cmp.Or(tc.method, http.MethodGet)
			requestLogHandler(tc.handler).ServeHTTP(w, httptest.NewRequest(method, "/b/k", nil))

			id := w.Header().Get("x-amz-request-id")
			assert.Len(t, id, 26)
			assert.Equal(t, tc.wantStatus, w.Code)
			assert.Contains(t, buf.String(), `msg="S3 API request" request_id=`+id+" method="+method+" path=/b/k ")
			assert.Contains(t, buf.String(), " status="+strconv.Itoa(tc.wantStatus)+" ")
			if tc.wantBytes != "" {
				assert.Contains(t, buf.String(), tc.wantBytes)
			}
			if tc.wantBodyID {
				assert.Contains(t, w.Body.String(), "<RequestId>"+id+"</RequestId>")
			}
		})
	}

	t.Run("each request gets its own ID", func(t *testing.T) {
		h := requestLogHandler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		w1, w2 := httptest.NewRecorder(), httptest.NewRecorder()
		h.ServeHTTP(w1, httptest.NewRequest(http.MethodGet, "/", nil))
		h.ServeHTTP(w2, httptest.NewRequest(http.MethodGet, "/", nil))
		assert.NotEqual(t, w1.Header().Get("x-amz-request-id"), w2.Header().Get("x-amz-request-id"))
	})
}

// readFromRecorder records whether a body reached its ReadFrom.
type readFromRecorder struct {
	*httptest.ResponseRecorder
	readFrom bool
}

func (w *readFromRecorder) ReadFrom(src io.Reader) (int64, error) {
	w.readFrom = true
	return io.Copy(w.ResponseRecorder, src)
}

func TestRequestLogHandlerReadFrom(t *testing.T) {
	buf := captureSlog(t)
	w := &readFromRecorder{ResponseRecorder: httptest.NewRecorder()}
	requestLogHandler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.Copy(w, io.LimitReader(strings.NewReader("hello world"), 11))
	})).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/b/k", nil))
	assert.True(t, w.readFrom)
	assert.Equal(t, "hello world", w.Body.String())
	assert.Contains(t, buf.String(), "status=200 bytes=11 ")
}

func TestSetS3OperationOutsideS3Handler(t *testing.T) {
	assert.NotPanics(t, func() { SetS3Operation(httptest.NewRequest(http.MethodGet, "/", nil), "GetObject") })
}

func TestS3HandlerRequestLog(t *testing.T) {
	registryMux.Lock()
	origS3 := s3Handlers
	s3Handlers = map[string]HandlerFunc{}
	registryMux.Unlock()
	defer func() {
		registryMux.Lock()
		s3Handlers = origS3
		registryMux.Unlock()
	}()

	RegisterS3HandleFunc("GET /{bucket}/{key...}", func(_ *Server, w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(RequestIDFromContext(r.Context())))
	})
	cfg := DefaultConfig()
	cfg.Root = t.TempDir()
	srv, err := NewServer(context.Background(), cfg)
	require.NoError(t, err)

	testCases := []struct {
		caseName   string
		method     string
		path       string
		wantLogged bool
	}{
		{caseName: "route", method: http.MethodGet, path: "/b/k", wantLogged: true},
		{caseName: "rejected path", method: http.MethodGet, path: "/b/./k", wantLogged: true},
		{caseName: "health check", method: http.MethodGet, path: "/healthz"},
		{caseName: "CORS preflight", method: http.MethodOptions, path: "/b/k"},
	}
	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			buf := captureSlog(t)
			w := httptest.NewRecorder()
			srv.S3Handler().ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))

			id := w.Header().Get("x-amz-request-id")
			if !tc.wantLogged {
				assert.Empty(t, id)
				assert.NotContains(t, buf.String(), "S3 API request")
				return
			}
			assert.Len(t, id, 26)
			assert.Contains(t, w.Body.String(), id)
			assert.Contains(t, buf.String(), "request_id="+id)
		})
	}
}

// captureSlog sends slog to a Debug-level buffer until t ends; SetDefault alone does not restore the log package.
func captureSlog(t *testing.T) *strings.Builder {
	buf := &strings.Builder{}
	prev, prevW, prevFlags := slog.Default(), log.Writer(), log.Flags()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() {
		slog.SetDefault(prev)
		log.SetOutput(prevW)
		log.SetFlags(prevFlags)
	})
	return buf
}
