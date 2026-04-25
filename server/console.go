package server

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"time"
)

// consoleHandlers holds the routes registered via RegisterConsoleHandleFunc,
// served by ConsoleHandler().
var consoleHandlers = map[string]HandlerFunc{}

// ConsoleHandler builds an HTTP handler that serves the Web Console.
func (s *Server) ConsoleHandler() http.Handler {
	return s.buildMux(consoleHandlers)
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
