package console

import (
	"bytes"
	"context"
	"net/http"

	"github.com/mojatter/s2/server"
	"github.com/mojatter/s2/server/middleware"
)

func handleIndex(s *server.Server, w http.ResponseWriter, r *http.Request) {
	if err := s.RenderConsoleIndex(r.Context(), w, nil); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func handleCreateBucket(s *server.Server, w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	name := r.FormValue("name")
	if name == "" {
		http.Error(w, "bucket name is required", http.StatusBadRequest)
		return
	}
	// ConsoleAction can only check a wildcard placeholder resource for
	// this route (the bucket doesn't exist in the URL and isn't known
	// until the form above is parsed), so re-check the real name here --
	// a Deny scoped to this specific bucket name must still apply.
	if !server.AllowedS3BucketAction(server.UserFromContext(r.Context()), server.ActionCreateBucket, name) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	if err := s.Buckets.Create(r.Context(), name); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	renderBucketList(r.Context(), s, w)
}

func handleDeleteBucket(s *server.Server, w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if name == "" {
		http.Error(w, "bucket name is required", http.StatusBadRequest)
		return
	}
	if err := s.Buckets.Delete(r.Context(), name); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	names, err := s.Buckets.Names(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	names = server.FilterBucketNames(server.UserFromContext(r.Context()), names)

	data := struct{ Buckets []string }{Buckets: names}

	var buf bytes.Buffer
	if err := s.Template.ExecuteTemplate(&buf, "console/buckets/list.html", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// OOB swap to reset main content to empty state
	buf.WriteString(`<div id="main-content" hx-swap-oob="innerHTML">`)

	if err := s.Template.ExecuteTemplate(&buf, "console/empty.html", nil); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	buf.WriteString(`</div>`)

	w.Header().Set("HX-Push-Url", "/")
	_, _ = buf.WriteTo(w)
}

// renderBucketList renders the sidebar bucket list fragment.
func renderBucketList(ctx context.Context, s *server.Server, w http.ResponseWriter) {
	names, err := s.Buckets.Names(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	names = server.FilterBucketNames(server.UserFromContext(ctx), names)

	data := struct{ Buckets []string }{Buckets: names}

	var buf bytes.Buffer
	if err := s.Template.ExecuteTemplate(&buf, "console/buckets/list.html", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_, _ = buf.WriteTo(w)
}

func init() {
	server.RegisterConsoleHandleFunc("GET /{$}", middleware.BasicAuth(handleIndex))
	server.RegisterConsoleHandleFunc("POST /buckets", middleware.BasicAuth(handleCreateBucket))
	server.RegisterConsoleHandleFunc("DELETE /buckets/{name}", middleware.BasicAuth(handleDeleteBucket))
}
