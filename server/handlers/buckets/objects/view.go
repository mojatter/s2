package objects

import (
	"fmt"
	"io"
	"mime"
	"net/http"
	"path"
	"strings"

	"github.com/mojatter/s2/server"
)

// contentTypeByExt returns the MIME type for the given file extension.
// It uses mime.TypeByExtension first, then falls back to a built-in map
// for common types that the OS mime database may not cover.
func contentTypeByExt(ext string) string {
	if ct := mime.TypeByExtension(ext); ct != "" {
		return ct
	}
	ext = strings.ToLower(ext)
	switch ext {
	case ".md":
		return "text/plain; charset=utf-8"
	case ".log", ".cfg", ".conf", ".ini":
		return "text/plain; charset=utf-8"
	case ".go", ".py", ".rb", ".rs", ".java", ".c", ".h", ".cpp", ".ts":
		return "text/plain; charset=utf-8"
	case ".sh", ".makefile", ".dockerfile":
		return "text/plain; charset=utf-8"
	case ".webp":
		return "image/webp"
	case ".flac":
		return "audio/flac"
	case ".wasm":
		return "application/wasm"
	}
	return "application/octet-stream"
}

func handleView(s *server.Server, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucketName := r.PathValue("name")
	objectName := r.PathValue("object")

	strg, err := s.Buckets.Get(ctx, bucketName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	obj, err := strg.Get(ctx, objectName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	rc, err := obj.Open()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rc.Close()

	ct := contentTypeByExt(path.Ext(objectName))
	w.Header().Set("Content-Type", ct)
	w.Header().Set("Content-Disposition", fmt.Sprintf("inline; filename=\"%s\"", path.Base(objectName)))

	if _, err := io.Copy(w, rc); err != nil {
		fmt.Printf("Error copying object content: %v\n", err)
	}
}

func init() {
	server.RegisterHandleFunc("GET /buckets/{name}/view/{object...}", handleView)
}
