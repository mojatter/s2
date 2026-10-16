package server

import (
	"context"
	"crypto/rand"
	"encoding/xml"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// S3ErrorResponse is the XML response body for an S3 API error, shared by
// both server/handlers/s3api (application-level errors) and
// server/middleware (SigV4 authentication/authorization errors) so the
// two don't drift into subtly different error shapes.
type S3ErrorResponse struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource"`
	RequestID string   `xml:"RequestId"`
}

// WriteS3Error writes code/message as an S3ErrorResponse with the given
// HTTP status.
func WriteS3Error(w http.ResponseWriter, r *http.Request, code, message string, status int) {
	WriteXML(w, status, S3ErrorResponse{
		Code:      code,
		Message:   message,
		Resource:  r.URL.Path,
		RequestID: RequestIDFromContext(r.Context()),
	})
}

type requestInfoKey struct{}

// requestInfo is what the request line logs beyond the HTTP exchange.
type requestInfo struct {
	id        string
	operation string
	attrs     []any
}

// RequestIDFromContext returns the ID the S3 API gave the request, or "" outside it.
func RequestIDFromContext(ctx context.Context) string {
	if info, ok := ctx.Value(requestInfoKey{}).(*requestInfo); ok {
		return info.id
	}
	return ""
}

// SetS3Operation names the S3 operation r serves, with attrs, on its request line; a later call replaces it.
func SetS3Operation(r *http.Request, name string, attrs ...any) {
	if info, ok := r.Context().Value(requestInfoKey{}).(*requestInfo); ok {
		info.operation, info.attrs = name, attrs
	}
}

// requestLogHandler gives each request an ID, answers it as x-amz-request-id and logs the request at Debug.
func requestLogHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		info := &requestInfo{id: rand.Text()}
		w.Header().Set("x-amz-request-id", info.id)
		rw := &responseRecorder{ResponseWriter: w}
		start := time.Now()
		r = r.WithContext(context.WithValue(r.Context(), requestInfoKey{}, info))
		next.ServeHTTP(rw, r)
		args := []any{"request_id", info.id, "method", r.Method, "path", r.URL.Path}
		if info.operation != "" {
			args = append(append(args, "operation", info.operation), info.attrs...)
		}
		args = append(args, "status", rw.statusCode(), "bytes", rw.bytes, "duration", time.Since(start))
		slog.DebugContext(r.Context(), "S3 API request", args...)
	})
}

// responseRecorder records the status and body size written through it.
type responseRecorder struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (rw *responseRecorder) WriteHeader(status int) {
	if rw.status == 0 {
		rw.status = status
	}
	rw.ResponseWriter.WriteHeader(status)
}

func (rw *responseRecorder) Write(p []byte) (int, error) {
	if rw.status == 0 {
		rw.status = http.StatusOK
	}
	n, err := rw.ResponseWriter.Write(p)
	rw.bytes += int64(n)
	return n, err
}

// ReadFrom keeps the underlying writer's ReadFrom, which serves a file body with sendfile.
func (rw *responseRecorder) ReadFrom(src io.Reader) (int64, error) {
	if rw.status == 0 {
		rw.status = http.StatusOK
	}
	n, err := io.Copy(rw.ResponseWriter, src)
	rw.bytes += n
	return n, err
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (rw *responseRecorder) Unwrap() http.ResponseWriter {
	return rw.ResponseWriter
}

// statusCode is the status sent, which is 200 when the handler wrote nothing.
func (rw *responseRecorder) statusCode() int {
	if rw.status == 0 {
		return http.StatusOK
	}
	return rw.status
}

// WriteXML writes v as an XML response body with the given HTTP status.
func WriteXML(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = fmt.Fprint(w, xml.Header)
	if err := xml.NewEncoder(w).Encode(v); err != nil {
		slog.Error("Failed to encode XML", "error", err)
	}
}

// s3Handlers holds the routes registered via RegisterS3HandleFunc,
// served by S3Handler().
var s3Handlers = map[string]HandlerFunc{}

// S3Handler serves the registered S3 routes behind a request ID and Debug line, the health endpoint (when cfg.HealthPath is set) and permissive CORS.
func (s *Server) S3Handler() http.Handler {
	reject := func(w http.ResponseWriter, r *http.Request) {
		WriteS3Error(w, r, "InvalidArgument", "The request path is not valid", http.StatusBadRequest)
	}
	return s.buildMux(s3Handlers, cleanPathHandler(reject), requestLogHandler, healthHandler(s.Config.HealthPath), corsHandler)
}

// cleanPathHandler answers with reject when the request path is not already
// the path it resolves to. ServeMux redirects a "." or ".." element it can
// see, but an encoded "..%2F" stays inside a single element and reaches a
// backend as a key that leaves the bucket; a "." or an empty element is
// milder but still resolves to a different name than it spells, which is how
// a policy written against the spelled name is evaded -- authorization reads
// the raw path, the backend reads the resolved one. reject differs per mux:
// the S3 API owes the client an XML error document, the console a plain one.
func cleanPathHandler(reject http.HandlerFunc) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			elems := strings.Split(r.URL.Path, "/")
			for i, elem := range elems {
				// The first and last elements are empty for the leading and
				// an optional trailing "/", which resolve to nothing.
				if elem == "." || elem == ".." || (elem == "" && i > 0 && i < len(elems)-1) {
					reject(w, r)
					return
				}
			}
			next.ServeHTTP(w, r)
		})
	}
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
		h.Set("Access-Control-Allow-Headers", "Authorization, *")
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
