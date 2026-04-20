package middleware

import (
	"net/http"

	"github.com/mojatter/s2/server"
)

// ServeConsoleIndex intercepts non-HTMX requests and returns the full index page,
// allowing client-side JS to restore fragment state from the URL (e.g. on reload).
func ServeConsoleIndex(next server.HandlerFunc) server.HandlerFunc {
	return func(srv *server.Server, w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("HX-Request") == "true" {
			next(srv, w, r)
			return
		}
		if err := srv.RenderConsoleIndex(w); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}
