package server

import "net/http"

var s3Handlers = map[string]HandlerFunc{}

// S3Handler builds an HTTP handler that serves the S3-compatible API.
// It includes routes registered via RegisterS3HandleFunc and, when
// cfg.HealthPath is non-empty, a health endpoint at that path.
func (s *Server) S3Handler() http.Handler {
	return corsHandler(s.buildMux(s3Handlers))
}

// RegisterS3HandleFunc registers a handler that will be served by
// S3Handler(). Patterns use Go 1.22 ServeMux syntax.
func RegisterS3HandleFunc(pattern string, handler HandlerFunc) {
	registerInto(s3Handlers, "S3 ", pattern, handler)
}

// handleHealthz is mounted at cfg.HealthPath by S3Handler. It is
// intentionally minimal so that probes stay cheap.
func handleHealthz(_ *Server, w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
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
