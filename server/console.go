package server

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/mojatter/s2"
)

// consoleHandlers holds the routes registered via RegisterConsoleHandleFunc,
// served by ConsoleHandler().
var consoleHandlers = map[string]HandlerFunc{}

// ConsoleHandler builds an HTTP handler that serves the Web Console.
func (s *Server) ConsoleHandler() http.Handler {
	reject := func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "the request path is not valid", http.StatusBadRequest)
	}
	return s.buildMux(consoleHandlers, cleanPathHandler(reject))
}

// RegisterConsoleHandleFunc registers a handler that will be served by
// ConsoleHandler(). Patterns use Go 1.22 ServeMux syntax.
func RegisterConsoleHandleFunc(pattern string, handler HandlerFunc) {
	registerHandler(consoleHandlers, "console ", pattern, handler)
}

// RenderConsoleIndex renders the full index.html page into w. The
// current bucket list is added to data under the "Buckets" key before
// template execution; data may be nil.
func (s *Server) RenderConsoleIndex(ctx context.Context, w http.ResponseWriter, data map[string]any) error {
	names, err := s.Buckets.Names(ctx)
	if err != nil {
		return err
	}
	names = FilterBucketNames(UserFromContext(ctx), names)
	if data == nil {
		data = map[string]any{}
	}
	data["Buckets"] = names
	var buf bytes.Buffer
	if err := s.Template.ExecuteTemplate(&buf, "console/index.html", data); err != nil {
		return err
	}
	_, _ = buf.WriteTo(w)
	return nil
}

// WriteConsoleError answers err with status and a body naming no server detail, which it logs instead.
func WriteConsoleError(w http.ResponseWriter, r *http.Request, status int, err error) {
	level := slog.LevelDebug
	// A client that hung up is not a server fault.
	if status >= http.StatusInternalServerError && !errors.Is(err, context.Canceled) {
		level = slog.LevelError
	}
	slog.Log(r.Context(), level, "Console error", "method", r.Method, "path", r.URL.Path, "status", status, "error", err)
	http.Error(w, consoleErrorBody(status, err), status)
}

// consoleErrorBody keeps the text s2 writes about the client's own names and answers the status text otherwise.
func consoleErrorBody(status int, err error) string {
	var notFound *ErrBucketNotFound
	switch {
	case errors.Is(err, s2.ErrInvalidName), errors.Is(err, ErrReservedBucketName):
		return err.Error()
	case errors.As(err, &notFound):
		return "bucket not found"
	case errors.Is(err, s2.ErrNotExist):
		return "object not found"
	case errors.Is(err, s2.ErrModified):
		// A retry reads the key's current version, as the S3 API tells its clients.
		return "object modified while it was being read; try again"
	}
	return http.StatusText(status)
}

func init() {
	registerHttpServerFactory(func(s *Server) *http.Server {
		if s.Config.ConsoleListen == "" {
			return nil
		}
		slog.Info("Web Console listening", "addr", s.Config.ConsoleListen)
		return &http.Server{
			Addr:              s.Config.ConsoleListen,
			Handler:           s.ConsoleHandler(),
			ReadHeaderTimeout: 30 * time.Second,
		}
	})
}
