package handlers

import (
	"bytes"
	"net/http"

	"github.com/mojatter/s2/server"
)

func handleIndex(s *server.Server, w http.ResponseWriter, r *http.Request) {
	names, err := s.Buckets.Names()

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	data := struct {
		Buckets []string
	}{
		Buckets: names,
	}

	var buf bytes.Buffer
	if err := s.Template.ExecuteTemplate(&buf, "index.html", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, _ = buf.WriteTo(w)
}

func handleCreateBucket(s *server.Server, w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	name := r.FormValue("name")
	if name == "" {
		http.Error(w, "bucket name is required", http.StatusBadRequest)
		return
	}
	if err := s.Buckets.Create(r.Context(), name); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// For HTMX, just redirect or re-render (usually redirect to index is fine)
	http.Redirect(w, r, "/", http.StatusFound)
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

	// For HTMX DELETE request, redirect to index
	w.Header().Set("HX-Redirect", "/")
	w.WriteHeader(http.StatusOK)
}


func init() {
	server.RegisterHandleFunc("GET /", handleIndex)
	server.RegisterHandleFunc("POST /buckets", handleCreateBucket)
	server.RegisterHandleFunc("DELETE /buckets/{name}", handleDeleteBucket)
	server.RegisterTemplate("index.html")
}

