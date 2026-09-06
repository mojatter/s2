package objects

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path"
	"strings"

	"github.com/mojatter/s2"
	"github.com/mojatter/s2/server"
	"github.com/mojatter/s2/server/middleware"
)

// userMetadata returns obj's metadata with s2's own internal bookkeeping
// keys (server.InternalMetadataKeys -- ETag, Content-Type) filtered out, so
// the metadata panel/preview page only ever shows what the client actually
// set via x-amz-meta-*, matching the S3 API's GetObject behavior.
func userMetadata(obj s2.Object) map[string]string {
	return server.FilterInternalMetadata(obj.Metadata())
}

// resolvedContentType returns obj's stored Content-Type, else a guess
// from the key's extension. Unlike the S3 API (see setContentType in the
// s3api package) it always ends with a concrete type: a browser renders
// this response, and no Content-Type turns a preview into a download.
func resolvedContentType(obj s2.Object, name string) string {
	if ct, ok := obj.Metadata().Get(server.ContentTypeMetadataKey); ok {
		return ct
	}
	if ct := server.ContentTypeByExt(path.Ext(name)); ct != "" {
		return ct
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
	defer func() { _ = rc.Close() }()

	w.Header().Set("Content-Type", resolvedContentType(obj, objectName))
	w.Header().Set("Content-Disposition", fmt.Sprintf("inline; filename=\"%s\"", path.Base(objectName)))
	// The type served here is the uploader's, so an object stored as
	// text/html would otherwise run on the console origin (#199).
	w.Header().Set("Content-Security-Policy", "sandbox")

	if _, err := io.Copy(w, rc); err != nil {
		slog.Error("Failed to copy object content", "error", err)
	}
}

func handleMeta(s *server.Server, w http.ResponseWriter, r *http.Request) {
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

	ct := resolvedContentType(obj, objectName)
	resp := map[string]any{
		"name":         path.Base(objectName),
		"contentType":  ct,
		"size":         obj.Length(),
		"lastModified": obj.LastModified().Format("2006-01-02 15:04:05"),
	}
	if md := userMetadata(obj); len(md) > 0 {
		resp["metadata"] = md
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func handlePreview(s *server.Server, w http.ResponseWriter, r *http.Request) {
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

	ext := strings.ToLower(path.Ext(objectName))
	viewURL := fmt.Sprintf("/buckets/%s/view/%s", bucketName, objectName)
	previewType := server.PreviewType(ext)

	var textContent string
	if previewType == "text" {
		rc, err := obj.Open()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer func() { _ = rc.Close() }()

		b, err := io.ReadAll(rc)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		textContent = string(b)
	}

	data := struct {
		Filename     string
		ViewURL      string
		ContentType  string
		Size         uint64
		LastModified string
		Metadata     map[string]string
		PreviewType  string
		TextContent  string
	}{
		Filename:     path.Base(objectName),
		ViewURL:      viewURL,
		ContentType:  resolvedContentType(obj, objectName),
		Size:         obj.Length(),
		LastModified: obj.LastModified().Format("2006-01-02 15:04:05"),
		PreviewType:  previewType,
		TextContent:  textContent,
	}
	if md := userMetadata(obj); len(md) > 0 {
		data.Metadata = md
	}

	var buf bytes.Buffer
	if err := s.Template.ExecuteTemplate(&buf, "console/buckets/preview.html", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = buf.WriteTo(w)
}

func init() {
	server.RegisterConsoleHandleFunc("GET /buckets/{name}/view/{object...}", middleware.BasicAuth(handleView))
	server.RegisterConsoleHandleFunc("GET /buckets/{name}/meta/{object...}", middleware.BasicAuth(handleMeta))
	server.RegisterConsoleHandleFunc("GET /buckets/{name}/preview/{object...}", middleware.BasicAuth(handlePreview))
}
