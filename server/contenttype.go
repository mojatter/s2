package server

import (
	"mime"
	"strings"
)

// DefaultContentType is what s2 stores and answers with when no
// Content-Type is known, matching S3's own default.
const DefaultContentType = "binary/octet-stream"

// ContentTypeByExt returns the MIME type for the given file extension,
// or "" when nothing recognizes it. It consults mime.TypeByExtension
// first, then a built-in table for common types the OS mime database may
// not cover.
func ContentTypeByExt(ext string) string {
	if ext == "" {
		return ""
	}
	if ct := mime.TypeByExtension(ext); ct != "" {
		return ct
	}
	// The switch is a fallback: mime.TypeByExtension varies by host and Go version.
	switch strings.ToLower(ext) {
	case ".md", ".log", ".cfg", ".conf", ".ini",
		".go", ".py", ".rb", ".rs", ".java", ".c", ".h", ".cpp", ".ts",
		".sh", ".makefile", ".dockerfile":
		return "text/plain; charset=utf-8"
	case ".webp":
		return "image/webp"
	case ".flac":
		return "audio/flac"
	case ".wasm":
		return "application/wasm"
	}
	return ""
}
