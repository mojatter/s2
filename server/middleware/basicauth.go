package middleware

import (
	"crypto/subtle"
	"net/http"

	"github.com/mojatter/s2/server"
)

// BasicAuth returns a handler that enforces HTTP Basic Auth for Web Console routes.
// Authentication is skipped when no credentials are configured (AuthEnabled false).
//
// Login itself is not policy-gated -- any configured user, restricted or
// not, may log in (matching MinIO's behavior). Once authenticated, each
// request is checked against the matched user's Policy (if any) using
// ConsoleAction, and the matched user is stashed on the request context
// (server.WithUser) so handlers such as the bucket-list page can filter
// their results by the same policy.
//
// The anonymous principal (server.AnonymousAccessKeyID) never matches here,
// even if a client sends it as the Basic Auth username with an empty
// password (which would otherwise compare equal, since the anonymous entry
// carries no SecretAccessKey by construction -- see Config.Validate).
// Anonymous access is an S3-API-only concept; the Web Console always
// requires a real credential.
func BasicAuth(next server.HandlerFunc) server.HandlerFunc {
	return func(srv *server.Server, w http.ResponseWriter, r *http.Request) {
		if !srv.Config.AuthEnabled() {
			next(srv, w, r)
			return
		}
		user, pass, ok := r.BasicAuth()
		u := srv.Config.LookupUser(user)
		if !ok || u == nil || u.AccessKeyID == server.AnonymousAccessKeyID || subtle.ConstantTimeCompare([]byte(pass), []byte(u.SecretAccessKey)) != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="s2"`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		action, resource := server.ConsoleAction(r)
		if !server.Authorized(u, action, resource) {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}

		next(srv, w, r.WithContext(server.WithUser(r.Context(), u)))
	}
}
