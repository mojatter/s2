package handlers

import (
	"io/fs"
	"net/http"
	"path"

	"github.com/mojatter/s2/server"
	"github.com/mojatter/s2/server/middleware"
)

func handleStatic(s *server.Server, w http.ResponseWriter, r *http.Request) {
	content, err := fs.ReadFile(server.StaticFS, "static/"+r.PathValue("filepath"))
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
	if _, err := w.Write(content); err != nil { // #nosec G705 -- content is from embed.FS, not user input
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func init() {
	server.RegisterHandleFunc("GET /static/{filepath...}", middleware.BasicAuth(handleStatic))
}
