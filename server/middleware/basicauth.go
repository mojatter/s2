package middleware

import (
	"crypto/subtle"
	"net/http"

	"github.com/mojatter/s2/server"
)

// BasicAuth returns a handler that enforces HTTP Basic Auth for Web Console routes.
// Authentication is skipped when User is not configured.
func BasicAuth(next server.HandlerFunc) server.HandlerFunc {
	return func(srv *server.Server, w http.ResponseWriter, r *http.Request) {
		if srv.Config.User == "" {
			next(srv, w, r)
			return
		}
		user, pass, ok := r.BasicAuth()
		if !ok ||
			subtle.ConstantTimeCompare([]byte(user), []byte(srv.Config.User)) != 1 ||
			subtle.ConstantTimeCompare([]byte(pass), []byte(srv.Config.Password)) != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="s2"`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		next(srv, w, r)
	}
}
