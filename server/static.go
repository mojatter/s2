package server

import (
	"embed"
	"io/fs"
	"net/http"
	"path"
	"strings"
)

//go:embed static/*
var staticFS embed.FS

func handleStatic(s *Server, w http.ResponseWriter, r *http.Request) {
	content, err := fs.ReadFile(staticFS, strings.TrimLeft(r.URL.Path, "/"))
	if err != nil {
		http.NotFound(w, r)
		return
	}

	contentType := "text/plain"
	switch path.Ext(r.URL.Path) {
	case ".css":
		contentType = "text/css"
	case ".js":
		contentType = "application/javascript"
	}

	w.Header().Set("Content-Type", contentType)
	w.Write(content)
}

func init() {
	RegisterHandleFunc("GET /static/{filepath...}", handleStatic)
}
