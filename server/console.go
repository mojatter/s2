package server

import (
	"bytes"
	"net/http"
)

var consoleHandlers = map[string]HandlerFunc{}

// ConsoleHandler builds an HTTP handler that serves the Web Console.
// Returns nil when no console routes have been registered, which lets
// the caller decide whether to start a second listener at all.
func (s *Server) ConsoleHandler() http.Handler {
	if len(consoleHandlers) == 0 {
		return nil
	}
	return s.buildMux(consoleHandlers)
}

// RegisterConsoleHandleFunc registers a handler that will be served by
// ConsoleHandler(). Patterns use Go 1.22 ServeMux syntax.
func RegisterConsoleHandleFunc(pattern string, handler HandlerFunc) {
	registerInto(consoleHandlers, "console ", pattern, handler)
}

// RenderConsoleIndex renders the full index.html page into w. The
// current bucket list is added to data under the "Buckets" key before
// template execution; data may be nil.
func (s *Server) RenderConsoleIndex(w http.ResponseWriter, data map[string]any) error {
	names, err := s.Buckets.Names()
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
