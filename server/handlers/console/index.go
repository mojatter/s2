package console

import (
	"net/http"

	"github.com/mojatter/s2/server"
	"github.com/mojatter/s2/server/middleware"
)

func handleIndex(s *server.Server, w http.ResponseWriter, r *http.Request) {
	if server.WantsFragment(w, r) {
		s.WriteConsoleTemplate(w, r, "console/empty.html", nil)
		return
	}
	if err := s.RenderConsoleIndex(r.Context(), w, nil); err != nil {
		server.WriteConsoleError(w, r, http.StatusInternalServerError, err)
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
	if !server.DenyUnlessAllowedS3BucketAction(w, server.UserFromContext(r.Context()), server.ActionCreateBucket, name) {
		return
	}
	if err := s.Buckets.Create(r.Context(), name); err != nil {
		server.WriteConsoleStorageError(w, r, err)
		return
	}

	renderBucketList(r, s, w)
}

func handleDeleteBucket(s *server.Server, w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if name == "" {
		http.Error(w, "bucket name is required", http.StatusBadRequest)
		return
	}
	if err := s.Buckets.Delete(r.Context(), name); err != nil {
		server.WriteConsoleStorageError(w, r, err)
		return
	}

	// The client resets the main view itself if it shows the bucket deleted.
	renderBucketList(r, s, w)
}

// renderBucketList renders the sidebar bucket list fragment.
func renderBucketList(r *http.Request, s *server.Server, w http.ResponseWriter) {
	ctx := r.Context()
	names, err := s.Buckets.Names(ctx)
	if err != nil {
		server.WriteConsoleError(w, r, http.StatusInternalServerError, err)
		return
	}
	names = server.FilterBucketNames(server.UserFromContext(ctx), names)

	s.WriteConsoleTemplate(w, r, "console/buckets/list.html", struct{ Buckets []string }{Buckets: names})
}

func init() {
	server.RegisterConsoleHandleFunc("GET /{$}", middleware.BasicAuth(handleIndex))
	server.RegisterConsoleHandleFunc("POST /buckets", middleware.BasicAuth(handleCreateBucket))
	server.RegisterConsoleHandleFunc("DELETE /buckets/{name}", middleware.BasicAuth(handleDeleteBucket))
}
