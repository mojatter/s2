package server

import (
	"log/slog"
	"net/http"
	"time"
)

// s3Handlers holds the routes registered via RegisterS3HandleFunc,
// served by S3Handler().
var s3Handlers = map[string]HandlerFunc{}

// S3Handler builds an HTTP handler that serves the S3-compatible API:
// routes registered via RegisterS3HandleFunc, wrapped with the health
// endpoint at cfg.HealthPath (when set) and permissive CORS headers.
func (s *Server) S3Handler() http.Handler {
	return s.buildMux(s3Handlers, healthHandler(s.Config.HealthPath), corsHandler)
}

// RegisterS3HandleFunc registers a handler that will be served by
// S3Handler(). Patterns use Go 1.22 ServeMux syntax.
func RegisterS3HandleFunc(pattern string, handler HandlerFunc) {
	registerHandler(s3Handlers, "S3 ", pattern, handler)
}

// corsHandler wraps next and adds permissive CORS headers to every response.
// OPTIONS preflight requests are answered immediately with 200 OK.
func corsHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Access-Control-Allow-Origin", "*")
		h.Set("Access-Control-Allow-Methods", "GET, PUT, POST, DELETE, HEAD, OPTIONS")
		h.Set("Access-Control-Allow-Headers", "*")
		h.Set("Access-Control-Max-Age", "86400")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// healthHandler returns a middleware that short-circuits GET/HEAD
// requests to path with a 200 OK "ok" response. When path is empty, the
// returned middleware is a no-op.
//
// The health endpoint is intentionally *not* registered as a pattern on
// the ServeMux: a literal path like "/healthz" and the broader
// "{METHOD} /{bucket}/{key...}" wildcards overlap in ways Go 1.22's
// conflict detector refuses to accept.
func healthHandler(path string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		if path == "" {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == path && (r.Method == http.MethodGet || r.Method == http.MethodHead) {
				w.Header().Set("Content-Type", "text/plain")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("ok"))
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func init() {
	registerHttpServerFactory(func(s *Server) *http.Server {
		slog.Info("S3 API listening", "addr", s.Config.Listen)
		return &http.Server{
			Addr:              s.Config.Listen,
			Handler:           s.S3Handler(),
			ReadHeaderTimeout: 30 * time.Second,
		}
	})
}
